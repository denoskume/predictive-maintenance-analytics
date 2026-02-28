-- 04_mart_views.sql
-- MART layer for BI (Power BI-ready)

CREATE SCHEMA IF NOT EXISTS mart;

-- Dimension: Machines
CREATE OR REPLACE VIEW mart.dim_machine AS
SELECT DISTINCT
  machine_id,
  machine_type
FROM analytics.predictive_maintenance_raw;

-- Dimension: Date
CREATE OR REPLACE VIEW mart.dim_date AS
SELECT DISTINCT
  DATE(timestamp) AS day
FROM analytics.predictive_maintenance_raw;

-- Fact: Daily machine KPIs
CREATE OR REPLACE VIEW mart.fact_machine_daily AS
SELECT
  DATE(timestamp) AS day,
  machine_id,
  machine_type,

  -- sensor aggregates
  AVG(vibration_rms) AS avg_vibration_rms,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY vibration_rms) AS p95_vibration_rms,

  AVG(temperature_motor) AS avg_temperature_motor,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY temperature_motor) AS p95_temperature_motor,

  AVG(current_phase_avg) AS avg_current_phase_avg,
  AVG(pressure_level) AS avg_pressure_level,
  AVG(rpm) AS avg_rpm,

  -- maintenance + environment
  AVG(hours_since_maintenance) AS avg_hours_since_maintenance,
  AVG(ambient_temp) AS avg_ambient_temp,

  -- RUL
  AVG(rul_hours) AS avg_rul_hours,
  MIN(rul_hours) AS min_rul_hours,

  -- outcomes
  AVG(failure_within_24h::numeric) AS failure_rate,
  SUM(failure_within_24h) AS failure_events,

  -- cost
  AVG(estimated_repair_cost) AS avg_estimated_repair_cost,

  -- expected risk cost (business KPI)
  (AVG(failure_within_24h::numeric) * AVG(estimated_repair_cost)) AS expected_risk_cost

FROM analytics.predictive_maintenance_raw
GROUP BY DATE(timestamp), machine_id, machine_type;

-- Machine-level summary (rank machines)
CREATE OR REPLACE VIEW mart.machine_summary AS
SELECT
  machine_id,
  machine_type,

  AVG(failure_within_24h::numeric) AS failure_rate,
  AVG(estimated_repair_cost) AS avg_estimated_repair_cost,
  AVG(rul_hours) AS avg_rul_hours,
  MIN(rul_hours) AS min_rul_hours,

  AVG(vibration_rms) AS avg_vibration_rms,
  AVG(temperature_motor) AS avg_temperature_motor,
  AVG(hours_since_maintenance) AS avg_hours_since_maintenance,

  (AVG(failure_within_24h::numeric) * AVG(estimated_repair_cost)) AS expected_risk_cost

FROM analytics.predictive_maintenance_raw
GROUP BY machine_id, machine_type;