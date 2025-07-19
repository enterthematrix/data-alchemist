-- This model analyzes genre trends over the years, summarizing the number of titles, average ratings, and votes per genre and year.
{{ config(
    materialized='incremental',
    unique_key=['genre', 'start_year'],
    incremental_strategy='merge'
) }}

with genre_data as (
    select
        ftg.genre_id,
        dg.genre,
        dt.start_year,
        fr.average_rating,
        fr.num_votes
    from {{ ref('fact_title_genre') }} ftg
    join {{ ref('dim_genre') }} dg on ftg.genre_id = dg.genre_id
    join {{ ref('dim_title') }} dt on ftg.title_id = dt.title_id
    left join {{ ref('fact_ratings') }} fr on ftg.title_id = fr.title_id
    where dt.start_year is not null
    {% if is_incremental() %}
      and dt.start_year >= (select max(start_year) from {{ this }})
    {% endif %}
)

select
    genre,
    start_year,
    count(*) as num_titles,
    round(avg(average_rating), 2) as avg_rating,
    round(avg(num_votes), 2) as avg_votes
from genre_data
group by genre, start_year
