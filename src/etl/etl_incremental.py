import os
import sys
import subprocess
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.etl.settings import SETTINGS
from src.etl.dq_checks import run_dq_checks

LOG_PATH = "logs/etl.log"

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs("logs", exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def connect():
    return psycopg2.connect(
        host=SETTINGS.db_host,
        port=SETTINGS.db_port,
        dbname=SETTINGS.db_name,
        user=SETTINGS.db_user,
        password=SETTINGS.db_password,
    )

def get_max_timestamp(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(timestamp) FROM analytics.predictive_maintenance_raw;")
        return cur.fetchone()[0]

def truncate_staging(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analytics.predictive_maintenance_staging;")
    conn.commit()

def copy_to_staging(conn, csv_path: str):
    # client-side copy using psycopg2 copy_expert (works on Windows)
    with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY analytics.predictive_maintenance_staging
            (timestamp, machine_id, machine_type, vibration_rms, temperature_motor,
             current_phase_avg, pressure_level, rpm, operating_mode, hours_since_maintenance,
             ambient_temp, rul_hours, failure_within_24h, failure_type, estimated_repair_cost)
            FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');
            """,
            f
        )
    conn.commit()

def upsert_from_staging(conn):
    """
    Returns: (inserted, updated)
    """
    with conn.cursor() as cur:
        # Count rows in staging
        cur.execute("SELECT COUNT(*) FROM analytics.predictive_maintenance_staging;")
        staged = cur.fetchone()[0]

        # Upsert: insert new keys; update existing keys
        # We can estimate inserted/updated via two queries around the upsert.
        cur.execute("SELECT COUNT(*) FROM analytics.predictive_maintenance_raw;")
        before = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO analytics.predictive_maintenance_raw (
              timestamp, machine_id, machine_type, vibration_rms, temperature_motor,
              current_phase_avg, pressure_level, rpm, operating_mode, hours_since_maintenance,
              ambient_temp, rul_hours, failure_within_24h, failure_type, estimated_repair_cost
            )
            SELECT
              timestamp, machine_id, machine_type, vibration_rms, temperature_motor,
              current_phase_avg, pressure_level, rpm, operating_mode, hours_since_maintenance,
              ambient_temp, rul_hours, failure_within_24h, failure_type, estimated_repair_cost
            FROM analytics.predictive_maintenance_staging
            ON CONFLICT (timestamp, machine_id) DO UPDATE SET
              machine_type = EXCLUDED.machine_type,
              vibration_rms = EXCLUDED.vibration_rms,
              temperature_motor = EXCLUDED.temperature_motor,
              current_phase_avg = EXCLUDED.current_phase_avg,
              pressure_level = EXCLUDED.pressure_level,
              rpm = EXCLUDED.rpm,
              operating_mode = EXCLUDED.operating_mode,
              hours_since_maintenance = EXCLUDED.hours_since_maintenance,
              ambient_temp = EXCLUDED.ambient_temp,
              rul_hours = EXCLUDED.rul_hours,
              failure_within_24h = EXCLUDED.failure_within_24h,
              failure_type = EXCLUDED.failure_type,
              estimated_repair_cost = EXCLUDED.estimated_repair_cost;
        """)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM analytics.predictive_maintenance_raw;")
        after = cur.fetchone()[0]

        inserted = max(after - before, 0)
        # updated rows are at most staged - inserted (approximation)
        updated = max(staged - inserted, 0)

    return inserted, updated

def refresh_mart():
    env = os.environ.copy()
    if SETTINGS.db_password:
        env["PGPASSWORD"] = SETTINGS.db_password

    cmd = [
        "psql",
        "-h", SETTINGS.db_host,
        "-p", str(SETTINGS.db_port),
        "-U", SETTINGS.db_user,
        "-d", SETTINGS.db_name,
        "-f", "sql/04_mart_views.sql"
    ]
    subprocess.check_call(cmd, env=env)

def write_run_history(conn, mode, rows_in_file, rows_loaded, inserted, updated, max_ts, status, message):
    with conn.cursor() as cur:
        cur.execute("""
          INSERT INTO analytics.etl_run_history
          (mode, rows_in_file, rows_loaded, rows_inserted, rows_updated, max_timestamp_in_db, status, message)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
        """, (mode, rows_in_file, rows_loaded, inserted, updated, max_ts, status, message))
    conn.commit()

def main():
    os.makedirs("logs", exist_ok=True)
    log("INCREMENTAL ETL START")

    csv_path = SETTINGS.csv_path
    if not os.path.exists(csv_path):
        log(f"ERROR: CSV not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    rows_in_file = len(df)

    issues = run_dq_checks(df)
    if issues:
        log("DQ FAILED")
        for i in issues:
            log(f"DQ_ISSUE: {i}")
        # best effort: write run history
        try:
            conn = connect()
            max_ts = get_max_timestamp(conn)
            write_run_history(conn, "incremental", rows_in_file, 0, 0, 0, max_ts, "FAILED", "DQ failed")
            conn.close()
        except Exception:
            pass
        sys.exit(2)

    conn = connect()
    try:
        max_ts_before = get_max_timestamp(conn)
        log(f"Max timestamp in DB before load: {max_ts_before}")

        # Optional filter: only keep rows newer than max_ts_before
        if max_ts_before is not None:
            df_new = df[df["timestamp"] > max_ts_before].copy()
            mode = "incremental_filtered"
        else:
            df_new = df.copy()
            mode = "initial_load"

        log(f"Rows in file: {rows_in_file} | Rows considered for load: {len(df_new)}")

        # If nothing new, stop early but still refresh mart optional
        if len(df_new) == 0:
            write_run_history(conn, mode, rows_in_file, 0, 0, 0, max_ts_before, "SUCCESS", "No new rows to load")
            log("No new rows. ETL SUCCESS.")
            return

        # Write filtered file to temp CSV for COPY (fast & safe)
        tmp_path = "data/processed/_etl_staging_tmp.csv"
        os.makedirs("data/processed", exist_ok=True)
        df_new.to_csv(tmp_path, index=False)

        truncate_staging(conn)
        copy_to_staging(conn, tmp_path)

        # Count loaded staging rows
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM analytics.predictive_maintenance_staging;")
            rows_loaded = cur.fetchone()[0]

        inserted, updated = upsert_from_staging(conn)
        max_ts_after = get_max_timestamp(conn)

        write_run_history(conn, mode, rows_in_file, rows_loaded, inserted, updated, max_ts_after, "SUCCESS", "OK")
        log(f"Loaded staging rows: {rows_loaded} | inserted≈{inserted} | updated≈{updated}")
        log(f"Max timestamp in DB after load: {max_ts_after}")

    finally:
        conn.close()

    refresh_mart()
    log("MART refreshed")
    log("INCREMENTAL ETL SUCCESS")

if __name__ == "__main__":
    main()