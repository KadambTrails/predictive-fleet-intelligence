
WITH raw_data AS (
    SELECT * FROM read_parquet('../data/raw_audit_table/*.parquet')
)

SELECT
    -- Metadata from the Bronze Layer
    log_id,
    CAST(raw_json_payload->>'$.timestamp' AS datetime) as event_timestamp,
    ingested_at,

    -- Core Logistics Fields
    raw_json_payload->>'$.company_id' as company_id,
    raw_json_payload->>'$.truck_id' as truck_id,
    raw_json_payload->>'$.driver_id' as driver_id,
    raw_json_payload->>'$.source_city' as source_city,
    raw_json_payload->>'$.dest_city' as dest_city,

    -- Telemetry Metrics (Cast to proper Numeric types)
    CAST(raw_json_payload->>'$.engine_temp' AS DOUBLE) as engine_temp,
    CAST(raw_json_payload->>'$.average_speed' AS DOUBLE) as avg_speed_mph,
    
    -- Convert 0.77 to 77.0 for easier reading
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
        WHEN raw_json_payload->>'$.weather_condition' IN ('hailstorm', 'snow', 'heavy rain') THEN TRUE
        ELSE FALSE
    END as hazardous_driving_flag

FROM raw_data