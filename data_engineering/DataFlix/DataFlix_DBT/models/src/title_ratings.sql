{{ config(
    materialized='table'
) }}

WITH staged AS (
    SELECT
        $1::STRING AS tconst,
        $2::FLOAT AS average_rating,
        $3::NUMBER AS num_votes
    FROM @{{ var('raw_schema') }}.{{ var('raw_stage') }}/{{ var('title_ratings') }} (file_format => dataflix_tsv_format)
)

SELECT * FROM staged
