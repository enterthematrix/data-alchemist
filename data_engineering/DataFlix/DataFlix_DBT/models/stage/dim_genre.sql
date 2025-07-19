{{ config(
    materialized='incremental'
) }}
with flat_genre as (
    select tconst,
    title_genres.value::string as genre_name,
    ingested_at
    from {{ ref('title_basics') }},
    lateral flatten(input => split(genres, ',')) as title_genres
    {% if is_incremental() %}
    where ingested_at > (select max(load_ts) from {{ this }})
    {% endif %}
),
de_duplicated_genres as (
    select distinct genre_name
    from flat_genre
),
unique_genres as (
    select  row_number() over (order by genre_name) as genre_id, 
        genre_name as genre,
        current_timestamp() as load_ts
from de_duplicated_genres
)
select * from unique_genres

