{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('name_basics') }}
),

known_for_split as (
    select
        nconst,
        primary_name,
        birth_year,
        death_year,
        trim(known_for_titles.value::string) as known_for_titles
    from base,
    lateral flatten(input => split(known_for_titles, ',')) as known_for_titles
)

select *
from known_for_split