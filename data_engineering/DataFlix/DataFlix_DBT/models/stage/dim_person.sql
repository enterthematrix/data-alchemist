{{ config(
    materialized='incremental'
) }}
with person_with_load_ts as (
select
    nconst as person_id,
    primary_name,
    birth_year,
    death_year, 
    ingested_at as load_ts
from {{ ref('name_basics') }}
),
person as (select * from person_with_load_ts)
select * from person
{% if is_incremental() %}
      where load_ts > (select max(load_ts) from {{ this }})
{% endif %}
