{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('name_basics') }}
),

profession_split as (
    select
        nconst,
        primary_name,
        birth_year,
        death_year,
        trim(profession.value::string) as profession
    from base,
    lateral flatten(input => split(primary_profession, ',')) as profession
)

select *
from profession_split
