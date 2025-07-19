{{ config(
    materialized='table' 
) }}

with staged as (
    select
        $1::string as title_id,
        $2::int as ordering,
        $3::string as nconst,
        $4::string as category,
        $5::string as job,
        $6::string as characters,
        current_timestamp() as ingested_at
    FROM@{{ var('raw_schema') }}.{{ var('raw_stage') }}/{{ var('title_principals') }} (file_format => dataflix_tsv_format)
)

SELECT * FROM staged