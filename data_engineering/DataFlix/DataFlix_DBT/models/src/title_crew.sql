{{ config(
    materialized='table'
) }}

WITH staged AS (
    SELECT
        $1::STRING AS tconst,
        $2::STRING AS director_ids,
        $3::STRING AS writer_ids,
        current_timestamp() as ingested_at
    FROM @{{ var('raw_schema') }}.{{ var('raw_stage') }}/{{ var('title_crew') }} (file_format => dataflix_tsv_format)
)

SELECT * FROM staged
