{{ config(
    materialized='incremental',
    unique_key=['RECORD_PK']
) }}

WITH clean_aqi_data AS (
    SELECT
        *,
        HASH(RECORD_TS) AS DATE_FK,
        hash(latitude, longitude) as LOCATION_FK,
        CALCULATE_AQI_UDF(
            PM10_AVG_VALUE,
            PM25_AVG_VALUE,
            SO2_AVG_VALUE,
            NO2_AVG_VALUE,
            NH3_AVG_VALUE,
            CO_AVG_VALUE,
            O3_AVG_VALUE
        ) AS AQI
    FROM {{ref('clean_aqi_data')}}
)
SELECT
        RECORD_PK,
        DATE_FK,
        LOCATION_FK,
        PM10_AVG_VALUE,
        PM25_AVG_VALUE,
        SO2_AVG_VALUE,
        NO2_AVG_VALUE,
        NH3_AVG_VALUE,
        CO_AVG_VALUE,
        O3_AVG_VALUE,
        GET_PROMINENT_POLLUTANT(
            PM10_AVG_VALUE,
            PM25_AVG_VALUE,
            SO2_AVG_VALUE,
            NO2_AVG_VALUE,
            NH3_AVG_VALUE,
            CO_AVG_VALUE,
            O3_AVG_VALUE
        ) AS PROMINENT_POLLUTANT,
        AQI,
        GET_AQI_CATEGORY(AQI) AS AQI_CATEGORY
FROM clean_aqi_data
{% if is_incremental() %}
WHERE record_pk NOT IN (
    SELECT record_pk FROM {{ this }}
)
{{ log('Loading ' ~ this ~ ' incrementally', info=True)}}
{% endif %}
ORDER BY DATE_FK DESC