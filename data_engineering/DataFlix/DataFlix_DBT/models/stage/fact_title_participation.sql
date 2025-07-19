{{ config(
    materialized='incremental'
) }}

with title_principals_ts as (
select
    title_id,
    nconst as person_id,
    category,
    job,
    characters,
    ingested_at as load_ts
from {{ ref('title_principals') }}
),
title_principals as (select * from title_principals_ts)
select * from title_principals
{% if is_incremental() %}
where load_ts > (select max(load_ts) from {{ this }})
{% endif %} 