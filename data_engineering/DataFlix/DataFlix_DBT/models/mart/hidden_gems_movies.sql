{{ config(materialized='table') }}

with base as (
    select
        dt.title_id,
        dt.primary_title,
        dt.start_year,
        ft.genres,
        dt.title_type,
        fr.average_rating,
        fr.num_votes,
        dt.runtime_minutes
    from {{ ref('dim_title') }} dt
    join {{ ref('fact_ratings') }} fr on dt.title_id = fr.title_id
    join {{ ref('fact_titles') }} ft ON ft.title_id = dt.title_id
    where dt.title_type = 'movie'
      and fr.average_rating >= 9.0
      and fr.num_votes between 2000 and 10000
      and dt.start_year is not null
)

select *
from base
order by average_rating desc, num_votes asc
