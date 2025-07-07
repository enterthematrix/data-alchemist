{{ config(
    materialized='incremental',
    unique_key=['RECORD_TS', 'LATITUDE', 'LONGITUDE']
) }}

WITH aqi_data AS (
    SELECT
        RECORD_TS,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        MAX(CASE WHEN POLLUTANT_ID = 'PM10' THEN POLLUTANT_AVG END) AS PM10_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'PM2.5' THEN POLLUTANT_AVG END) AS PM25_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'SO2' THEN POLLUTANT_AVG END) AS SO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NO2' THEN POLLUTANT_AVG END) AS NO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NH3' THEN POLLUTANT_AVG END) AS NH3_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'CO' THEN POLLUTANT_AVG END) AS CO_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'OZONE' THEN POLLUTANT_AVG END) AS O3_AVG
    FROM {{ source('aqi_source', 'raw_aqi_data') }}
    GROUP BY RECORD_TS, COUNTRY, STATE, CITY, STATION, LATITUDE, LONGITUDE
),

aqi_data_imputed AS (
    SELECT 
        *,
        -- Impute missing pollutant values using the macro defined in macros/impute_missing_pollutant_values.sql
        {{ impute_missing_pollutant_values('PM10_AVG') }} AS PM10_AVG_VALUE,
        {{ impute_missing_pollutant_values('PM25_AVG') }} AS PM25_AVG_VALUE,
        {{ impute_missing_pollutant_values('SO2_AVG') }} AS SO2_AVG_VALUE,
        {{ impute_missing_pollutant_values('NO2_AVG') }} AS NO2_AVG_VALUE,
        {{ impute_missing_pollutant_values('NH3_AVG') }} AS NH3_AVG_VALUE,
        {{ impute_missing_pollutant_values('CO_AVG') }} AS CO_AVG_VALUE,
        {{ impute_missing_pollutant_values('O3_AVG') }} AS O3_AVG_VALUE
    FROM aqi_data
)
SELECT
        RECORD_TS,
        HASH(RECORD_TS, LATITUDE, LONGITUDE) AS record_pk,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        PM10_AVG_VALUE,
        PM25_AVG_VALUE,
        SO2_AVG_VALUE,
        NO2_AVG_VALUE,
        NH3_AVG_VALUE,
        CO_AVG_VALUE,
        O3_AVG_VALUE     
FROM aqi_data_imputed
{% if is_incremental() %}
WHERE HASH(RECORD_TS,LATITUDE,LONGITUDE) NOT IN (
    SELECT record_pk FROM {{ this }}
)
{{ log('Loading ' ~ this ~ ' incrementally', info=True)}}
{% endif %}
ORDER BY RECORD_TS desc