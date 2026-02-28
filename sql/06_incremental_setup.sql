CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS mart;

-- 1) Table cible (raw) avec clé technique
-- On utilise (timestamp, machine_id) comme clé (standard sur séries temporelles machine).
-- Si tu as plusieurs lignes par machine au même timestamp, dis-moi → je te donne une clé hash.
CREATE TABLE IF NOT EXISTS analytics.predictive_maintenance_raw (
    timestamp TIMESTAMP NOT NULL,
    machine_id TEXT NOT NULL,
    machine_type TEXT,
    vibration_rms DOUBLE PRECISION,
    temperature_motor DOUBLE PRECISION,
    current_phase_avg DOUBLE PRECISION,
    pressure_level DOUBLE PRECISION,
    rpm DOUBLE PRECISION,
    operating_mode TEXT,
    hours_since_maintenance DOUBLE PRECISION,
    ambient_temp DOUBLE PRECISION,
    rul_hours DOUBLE PRECISION,
    failure_within_24h INTEGER,
    failure_type TEXT,
    estimated_repair_cost DOUBLE PRECISION,
    PRIMARY KEY (timestamp, machine_id)
);

-- 2) Staging table (chargée à chaque run)
DROP TABLE IF EXISTS analytics.predictive_maintenance_staging;
CREATE TABLE analytics.predictive_maintenance_staging (LIKE analytics.predictive_maintenance_raw INCLUDING ALL);

-- 3) Historique ETL (audit)
CREATE TABLE IF NOT EXISTS analytics.etl_run_history (
    run_id BIGSERIAL PRIMARY KEY,
    run_ts TIMESTAMP NOT NULL DEFAULT NOW(),
    mode TEXT NOT NULL,
    rows_in_file BIGINT,
    rows_loaded BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    max_timestamp_in_db TIMESTAMP,
    status TEXT NOT NULL,
    message TEXT
);

-- 4) Index utile (accélère queries et incrémental)
CREATE INDEX IF NOT EXISTS idx_pm_raw_machine_day
ON analytics.predictive_maintenance_raw (machine_id, timestamp);