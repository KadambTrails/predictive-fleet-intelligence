{{ config(error_if = ">0", warn_if = ">0") }}

SELECT
    log_id,
    avg_speed_mph,
    engine_temp
FROM {{ ref('silver_truck_data') }}
WHERE avg_speed_mph > 50.0 AND engine_temp < 100.0

-- This test checks for impossible hardware conditions.
-- If this query returns ANY rows, the pipeline test fails.