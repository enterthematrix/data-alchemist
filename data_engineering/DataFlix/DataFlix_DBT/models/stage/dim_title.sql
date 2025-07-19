{{ config(
    materialized='incremental'
) }}
with titles_ts as (
select
    tconst as title_id,
    title_type,
    primary_title,
    original_title,
    is_adult,
    start_year,
    end_year,
    runtime_minutes,
    ingested_at as load_ts
from {{ ref('title_basics') }}
),
titles as (select * from titles_ts)
select * from titles
{% if is_incremental() %}
where load_ts > (select max(load_ts) from {{ this }})
{% endif %} 

