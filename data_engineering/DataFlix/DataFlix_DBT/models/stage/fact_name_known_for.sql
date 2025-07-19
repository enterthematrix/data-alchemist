{{ config(
    materialized='incremental'
) }}

with base as (
    select nconst, 
    known_for_titles,
    ingested_at
    from {{ ref('name_basics') }}
    {% if is_incremental() %}
        where ingested_at > (select max(load_ts) from {{ this }})
    {% endif %}
),

known_for_split as (
    select
        nconst as person_id,
        trim(known_for_titles.value::string) as title_id
    from base,
    lateral flatten(input => split(known_for_titles, ',')) as known_for_titles
)

select
    person_id,
    title_id,
    current_timestamp() as load_ts
from known_for_split
