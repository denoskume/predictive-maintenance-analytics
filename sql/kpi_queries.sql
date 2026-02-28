-- kpi_queries.sql
-- Core KPI queries (industrial standard)

-- Failure rate by machine
SELECT
  machine_id,
  AVG(failure_within_24h::numeric) AS failure_rate
FROM predictive_maintenance
GROUP BY machine_id
ORDER BY failure_rate DESC;

-- Failure rate by machine type
SELECT
  machine_type,
  AVG(failure_within_24h::numeric) AS failure_rate
FROM predictive_maintenance
GROUP BY machine_type
ORDER BY failure_rate DESC;

-- Average repair cost by machine
SELECT
  machine_id,
  AVG(estimated_repair_cost) AS avg_repair_cost
FROM predictive_maintenance
GROUP BY machine_id
ORDER BY avg_repair_cost DESC;
