{{ config(
    materialized='incremental'
) }}
with genres_per_title as (
    select
        tconst as title_id,
        title_type,
        primary_title,
        original_title,
        is_adult,
        start_year,
        end_year,
        runtime_minutes,
        title_genres.value::string as genre,
        ingested_at as load_ts
    from {{ ref('title_basics') }},
    lateral flatten(input => split(genres, ',')) as title_genres
    {% if is_incremental() %}
        where ingested_at > (select max(load_ts) from {{ this }})
    {% endif %}
   
) select 
        title_id,
        dg.genre_id,
        load_ts 
        from genres_per_title
join {{ ref('dim_genre') }} dg
using (genre)
{% if is_incremental() %}
where load_ts > (select max(load_ts) from {{ this }})
{% endif %}