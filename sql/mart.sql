-- mart.sql
-- Analytics-ready view for Power BI (daily aggregation)

CREATE OR REPLACE VIEW mart_machine_daily AS
SELECT
  DATE(timestamp) AS day,
  machine_id,
  machine_type,
  AVG(vibration_rms) AS avg_vibration_rms,
  AVG(temperature_motor) AS avg_temperature_motor,
  AVG(current_phase_avg) AS avg_current_phase_avg,
  AVG(pressure_level) AS avg_pressure_level,
  AVG(rpm) AS avg_rpm,
  AVG(hours_since_maintenance) AS avg_hours_since_maintenance,
  AVG(ambient_temp) AS avg_ambient_temp,
  AVG(rul_hours) AS avg_rul_hours,
  AVG(failure_within_24h::numeric) AS failure_rate,
  AVG(estimated_repair_cost) AS avg_estimated_repair_cost
FROM predictive_maintenance
GROUP BY DATE(timestamp), machine_id, machine_type;
