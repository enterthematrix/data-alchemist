{# 
-- This model summarizes title information, including title type, start year, end year, runtime, average rating, number of votes, and genre. 
-- It is used to provide a quick overview of titles in the DataFlix dataset, which can be filtered by title type (movie/series), start year, and genre. 
-- The model is incremental and merges new data based on the `title_id`, clustering by `start_year` and `genre_id` for efficient querying. 
-- It also includes a timestamp for when the data was loaded. 
#}

top category (actor/director/...)
{{ config(
    materialized='incremental',
    unique_key='title_id',
    incremental_strategy='merge',
    cluster_by=['start_year', 'genre_id']
) }}

with base_titles as (
    select
        t.title_id,
        t.primary_title,
        t.title_type,
        t.start_year,
        t.end_year,
        t.runtime_minutes,
        t.is_adult,
        tr.average_rating,
        tr.num_votes,
        ftg.genre_id,
        dg.genre,
        current_timestamp() as load_ts
    from {{ ref('dim_title') }} t
    left join {{ ref('fact_ratings') }} tr on t.title_id = tr.title_id
    left join {{ ref('fact_title_genre') }} ftg on t.title_id = ftg.title_id
    left join {{ ref('dim_genre') }} dg on ftg.genre_id = dg.genre_id
    {% if is_incremental() %}
      where t.ingested_at > (select max(load_ts) from {{ this }})
    {% endif %}
)

select * from base_titles
