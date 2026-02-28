-- 03_data_quality.sql
-- Basic data quality checks

-- Row count
SELECT COUNT(*) AS row_count
FROM analytics.predictive_maintenance_raw;

-- Nulls per column (key columns)
SELECT
  SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
  SUM(CASE WHEN machine_id IS NULL THEN 1 ELSE 0 END) AS null_machine_id,
  SUM(CASE WHEN machine_type IS NULL THEN 1 ELSE 0 END) AS null_machine_type,
  SUM(CASE WHEN failure_within_24h IS NULL THEN 1 ELSE 0 END) AS null_failure_flag
FROM analytics.predictive_maintenance_raw;

-- Unique machines / types
SELECT
  COUNT(DISTINCT machine_id) AS machines,
  COUNT(DISTINCT machine_type) AS machine_types
FROM analytics.predictive_maintenance_raw;

-- Failure rate global
SELECT AVG(failure_within_24h::numeric) AS failure_rate_global
FROM analytics.predictive_maintenance_raw;