-- 02_load_csv.sql
-- Update the path to your local file


COPY analytics.predictive_maintenance_raw
FROM 'C:/Users/denos/Desktop/PROJECTS/predictive-maintenance-analytics/data/raw/predictive_maintenance.csv'
DELIMITER ','
CSV HEADER;