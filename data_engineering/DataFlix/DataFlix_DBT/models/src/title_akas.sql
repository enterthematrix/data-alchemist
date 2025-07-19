{{ config(
    materialized='table'
) }}

WITH staged AS (
    SELECT
        $1::STRING AS title_id,
        $2::NUMBER AS ordering,
        $3::STRING AS title,
        $4::STRING AS region,
        $5::STRING AS language,
        $6::STRING AS types,
        $7::STRING AS attributes,
        $8::BOOLEAN AS is_original_title,
        current_timestamp() as ingested_at
    FROM @{{ var('raw_schema') }}.{{ var('raw_stage') }}/{{ var('title_akas') }} (file_format => dataflix_tsv_format)
)

SELECT * FROM staged
