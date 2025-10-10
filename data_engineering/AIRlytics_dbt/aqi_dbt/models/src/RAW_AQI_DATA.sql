{{ config(
    materialized='incremental',
    unique_key='record_id',
    post_hook="ALTER TABLE {{ this }} CLUSTER BY LINEAR(station, TO_DATE(record_ts))"
) }}

WITH raw_data AS (
    SELECT 
        TRY_TO_TIMESTAMP($1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') AS record_ts,
        $1:records AS json_data,
        metadata$filename AS _stg_file_name,
        metadata$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        metadata$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts
    FROM @raw.aqi_raw_data_stg (file_format => AQI.RAW.JSON_FILE_FORMAT)
    -- If the file checksum is already present in the target table, skip it
    -- This is to avoid reprocessing files that have already been loaded
    {% if is_incremental() %}
        WHERE metadata$FILE_CONTENT_KEY NOT IN (
            SELECT DISTINCT _stg_file_md5 FROM {{ this }}
        )
    {% endif %}
),

flattened AS (
    SELECT 
        hourly_rec.value:country::text AS country,
        hourly_rec.value:state::text AS state,
        hourly_rec.value:city::text AS city,
        hourly_rec.value:station::text AS station,
        hourly_rec.value:latitude::number(12,7) AS latitude,
        hourly_rec.value:longitude::number(12,7) AS longitude,
        hourly_rec.value:pollutant_id::text AS pollutant_id,
        hourly_rec.value:max_value::text AS pollutant_max,
        hourly_rec.value:min_value::text AS pollutant_min,
        hourly_rec.value:avg_value::text AS pollutant_avg,
        raw_data._stg_file_name,
        raw_data._stg_file_load_ts,
        raw_data._stg_file_md5,
        raw_data._copy_data_ts,
        raw_data.record_ts,
        HASH(station, pollutant_id, TO_VARCHAR(raw_data.record_ts, 'YYYY-MM-DD HH24:MI:SS')) AS record_id
    FROM raw_data,
         LATERAL FLATTEN(input => raw_data.json_data) AS hourly_rec
)
SELECT * FROM flattened
