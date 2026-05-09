{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM read_parquet('../data/raw_audit_table/*.parquet')
)

SELECT
    -- Metadata
    log_id,
    (raw_json_payload->>'$.timestamp')::TIMESTAMP as event_timestamp,
    ingested_at,

    -- Core Logistics Fields
    raw_json_payload->>'$.company_id' as company_id,
    raw_json_payload->>'$.truck_id' as truck_id,
    raw_json_payload->>'$.driver_id' as driver_id,
    raw_json_payload->>'$.source_city' as source_city,
    raw_json_payload->>'$.dest_city' as dest_city,

    -- Telemetry Metrics
    CAST(raw_json_payload->>'$.engine_temp' AS DOUBLE) as engine_temp,
    CAST(raw_json_payload->>'$.average_speed' AS DOUBLE) as avg_speed_mph,
    CAST(raw_json_payload->>'$.fuel_remaining_percent' AS DOUBLE) * 100 as fuel_percent,

    -- Environmental & Load Data
    raw_json_payload->>'$.weather_condition' as weather,
    CAST(raw_json_payload->>'$.cargo_weight_kg' AS INTEGER) as cargo_weight_kg,
    raw_json_payload->>'$.driver_note' as driver_note,

    -- AI / Business Logic Features
    CASE 
        WHEN CAST(raw_json_payload->>'$.engine_temp' AS DOUBLE) > 210 THEN 'CRITICAL'
        WHEN CAST(raw_json_payload->>'$.engine_temp' AS DOUBLE) > 195 THEN 'WARNING'
        ELSE 'OPTIMAL'
    END as thermal_status,

    CASE 
        WHEN raw_json_payload->>'$.weather_condition' IN ('hailstorm', 'snow', 'heavy rain') THEN 1
        ELSE 0
    END as hazardous_driving_flag,

    (CAST(raw_json_payload->>'$.cargo_weight_kg' AS DOUBLE) * CAST(raw_json_payload->>'$.engine_temp' AS DOUBLE)) / 1000 as engine_stress_index
   
FROM raw_data
