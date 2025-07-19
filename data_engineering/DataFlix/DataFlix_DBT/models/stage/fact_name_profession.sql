{{ config(materialized='table') }}

with base as (
    select nconst, 
    primary_profession,
    ingested_at
    from {{ ref('name_basics') }}
    {% if is_incremental() %}
        where ingested_at > (select max(load_ts) from {{ this }})
    {% endif %}
),

profession_split as (
    select
        nconst as person_id,
        trim(lower(profession.value::string)) as profession
    from base,
    lateral flatten(input => split(primary_profession, ',')) as profession
)
select
    person_id,
    profession,
    current_timestamp() as load_ts
from profession_split
