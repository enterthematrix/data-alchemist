{{ config(
    materialized='incremental'
) }}


with staged as (
    select
        $1::string as nconst,
        $2::string as primary_name,
        $3::int as birth_year,
        $4::int as death_year,
        $5::string as primary_profession,
        $6::string as known_for_titles,
        current_timestamp() as ingested_at
    FROM @{{ var('raw_schema') }}.{{ var('raw_stage') }}/{{ var('name_basics') }} (file_format => dataflix_tsv_format)
)
select * from staged
