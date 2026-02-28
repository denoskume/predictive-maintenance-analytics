-- 01_create_schema.sql
-- Predictive Maintenance Analytics (PostgreSQL)

CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.predictive_maintenance_raw;

CREATE TABLE analytics.predictive_maintenance_raw (
    timestamp TIMESTAMP,
    machine_id TEXT,
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
    estimated_repair_cost DOUBLE PRECISION
);