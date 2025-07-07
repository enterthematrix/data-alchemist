-- This model aggregates daily AQI data per city, calculating average pollutant values and determining the
-- prominent pollutant and AQI category for each day.
-- It joins the fact table with dimension tables for measurement time and locations to provide a comprehensive view
-- of air quality data across different cities.
-- The model is configured for incremental loading, ensuring that only new or updated records are processed.

{{ config(
    materialized='incremental',
    unique_key=['record_pk']
) }}

WITH enriched_aqi_facts AS (
    SELECT
        HASH(aqi_location.CITY, aqi_time.AQI_YEAR, aqi_time.AQI_MONTH, aqi_time.AQI_DAY ) AS RECORD_PK,
        aqi_fact.location_fk,
        aqi_time.AQI_DAY AS DAY,
        aqi_time.AQI_MONTH AS MONTH,
        aqi_time.AQI_YEAR AS YEAR,
        aqi_fact.PM10_AVG_VALUE,
        aqi_fact.PM25_AVG_VALUE,
        aqi_fact.SO2_AVG_VALUE,
        aqi_fact.NO2_AVG_VALUE,
        aqi_fact.NH3_AVG_VALUE,
        aqi_fact.CO_AVG_VALUE,
        aqi_fact.O3_AVG_VALUE,
        aqi_fact.AQI,
        GET_PROMINENT_POLLUTANT(
            PM10_AVG_VALUE,
            PM25_AVG_VALUE,
            SO2_AVG_VALUE,
            NO2_AVG_VALUE,
            NH3_AVG_VALUE,
            CO_AVG_VALUE,
            O3_AVG_VALUE
        ) AS PROMINENT_POLLUTANT,
        GET_AQI_CATEGORY(AQI) AS AQI_CATEGORY
    FROM {{ ref('fact_aqi') }} AS aqi_fact
    JOIN {{ ref('dim_measurement_time') }} AS aqi_time
    ON aqi_fact.date_fk = aqi_time.date_pk
    JOIN {{ ref('dim_locations') }} AS aqi_location
    ON aqi_fact.location_fk = aqi_location.location_pk
)
SELECT
    RECORD_PK,
    location_fk,
    YEAR,
    MONTH,
    DAY,
    ROUND(AVG(PM10_AVG_VALUE)) AS PM10_AVG,
    ROUND(AVG(PM25_AVG_VALUE)) AS PM25_AVG,
    ROUND(AVG(SO2_AVG_VALUE)) AS SO2_AVG,
    ROUND(AVG(NO2_AVG_VALUE)) AS NO2_AVG,
    ROUND(AVG(O3_AVG_VALUE)) AS O3_AVG,
    ROUND(AVG(CO_AVG_VALUE)) AS CO_AVG,
    MAX(PROMINENT_POLLUTANT) AS PROMINENT_POLLUTANT,
    MAX(AQI_CATEGORY) AS AQI_CATEGORY

FROM enriched_aqi_facts
 {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} existing WHERE existing.RECORD_PK = enriched_aqi_facts.RECORD_PK)
    {{ log('Loading incrementally: ' ~ this, info=True) }}
    {% endif %}
GROUP BY RECORD_PK,location_fk, YEAR, MONTH, DAY
ORDER BY YEAR, MONTH, DAY
