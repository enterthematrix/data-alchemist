{{ config(
    materialized='table'
) }}

WITH staged AS (
    SELECT
        $1::STRING AS tconst,
        $2::STRING AS title_type,
        $3::STRING AS primary_title,
        $4::STRING AS original_title,
        $5::BOOLEAN AS is_adult,
        $6::NUMBER AS start_year,
        $7::NUMBER AS end_year,
        $8::NUMBER AS runtime_minutes,
        $9::STRING AS genres,
        current_timestamp() as ingested_at
    FROM @{{ var('raw_schema') }}.{{ var('raw_stage') }}/{{ var('title_basics') }} (file_format => dataflix_tsv_format)
)

SELECT * FROM staged