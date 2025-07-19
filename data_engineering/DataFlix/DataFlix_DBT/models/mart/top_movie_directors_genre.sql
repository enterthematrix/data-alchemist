{{ config(materialized='table') }}

with base as (

    select
        dp.person_id,
        dp.primary_name as director_name,
        dt.title_id,
        dt.primary_title,
        fr.average_rating,
        fr.num_votes,
        ft.genres,
        dt.start_year
    from {{ ref('fact_title_participation') }} ftp
    join {{ ref('dim_person') }} dp on ftp.person_id = dp.person_id
    join {{ ref('dim_title') }} dt on ftp.title_id = dt.title_id
    join {{ ref('fact_ratings') }} fr on dt.title_id = fr.title_id
    join {{ ref('fact_titles') }} ft on ft.title_id = dt.title_id
    where ftp.category = 'director'
      and ft.title_type = 'movie'
      and fr.average_rating is not null
      and fr.num_votes is not null
      and ft.genres is not null
),

exploded as (
    select 
        person_id,
        director_name,
        title_id,
        primary_title,
        average_rating,
        num_votes,
        start_year,
        trim(genre_flattened.value) as genre
    from base,
    lateral flatten(input => split(genres, ',')) as genre_flattened
),

aggregated as (
    select
        person_id,
        director_name,
        genre,
        count(distinct title_id) as num_titles,
        avg(average_rating) as avg_rating,
        sum(num_votes) as total_votes,
        listagg(primary_title, ', ') within group (order by average_rating desc) as top_titles
    from exploded
    group by person_id, director_name, genre
),

filtered as (
    select *
    from aggregated
    where num_titles >= 3
      and avg_rating >= 7.5
      and total_votes >= 10000
),

ranked as (
    select *,
           dense_rank() over (partition by genre order by avg_rating desc, total_votes desc) as rank
    from filtered
)

select *
from ranked
where rank <= 3
--and genre='Animation'
order by genre, rank
