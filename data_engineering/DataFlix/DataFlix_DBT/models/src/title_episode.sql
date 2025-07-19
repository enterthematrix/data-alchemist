{{ config(
    materialized='table' 
) }}

WITH staged AS (
    SELECT
        $1::STRING AS title_id,
        $2::STRING AS parent_title_id,
        $3::NUMBER AS season_number,
        $4::NUMBER AS episode_number,
        current_timestamp() as ingested_at
    FROM @{{ var('raw_schema') }}.{{ var('raw_stage') }}/{{ var('title_episode') }} (file_format => dataflix_tsv_format)
)
SELECT * FROM staged
