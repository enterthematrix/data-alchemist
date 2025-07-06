-- create a dimension table for measurement time, which includes the date and time of the air quality measurements.
{{ config(
    materialized='incremental',
    unique_key='date_pk'
) }}

WITH hourly_measurement_time AS (
    SELECT 
        RECORD_TS AS measurement_time,
        YEAR(RECORD_TS) AS aqi_year,
        QUARTER(RECORD_TS) AS aqi_quarter,
        MONTH(RECORD_TS) AS aqi_month,
        DAY(RECORD_TS) AS aqi_day,
        DAYOFWEEK(measurement_time) AS day_of_week,
        CASE WHEN DAYOFWEEK(measurement_time) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        HOUR(RECORD_TS) AS aqi_hour
    FROM {{ ref('clean_aqi_data') }} group by RECORD_TS, YEAR(RECORD_TS),  MONTH(RECORD_TS), DAY(RECORD_TS), HOUR(RECORD_TS)
)
SELECT
    HASH(measurement_time) AS date_pk,
    measurement_time,
    aqi_year,
    aqi_month,
    aqi_day,
    day_of_week,
    is_weekend,
    aqi_hour
FROM hourly_measurement_time
{% if is_incremental() %}
WHERE HASH(measurement_time) NOT IN (
    SELECT date_pk FROM {{ this }}
)
{{ log('Loading incrementally: ' ~ this, info=True) }}
{% endif %}
ORDER BY measurement_time DESC
