-- create_table.sql
-- PostgreSQL schema for predictive maintenance dataset

CREATE TABLE IF NOT EXISTS predictive_maintenance (
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
