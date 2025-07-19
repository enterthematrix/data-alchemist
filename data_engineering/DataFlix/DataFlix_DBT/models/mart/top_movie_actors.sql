{{ config(materialized='table') }}

with joined as (
    select
        dp.person_id,
        dp.primary_name as person_name,
        'actor' as role,
        dt.title_id,
        dt.primary_title,
        dt.start_year,
        fr.average_rating,
        fr.num_votes
    from {{ ref('fact_titles') }} ft
    join {{ ref('dim_title') }} dt on ft.title_id = dt.title_id
    join {{ ref('fact_ratings') }} fr on dt.title_id = fr.title_id
    join {{ ref('fact_title_participation') }} ftp on ftp.title_id = dt.title_id
    join {{ ref('dim_person') }} dp on ftp.person_id = dp.person_id
    where fr.average_rating is not null
      and fr.num_votes is not null
      and ftp.category in ('actor', 'actress')
      and ft.title_type = 'movie'
)

, ranked_titles as (
    select *,
           row_number() over (partition by person_id order by average_rating desc, num_votes desc) as rn
    from joined
)

, top_titles as (
    select person_id,
           listagg(primary_title, ', ') as top_titles
    from ranked_titles
    where rn <= 3
    group by person_id
)

, agg as (
    select
        j.person_id,
        j.person_name,
        j.role,
        count(distinct j.title_id) as num_titles,
        avg(j.average_rating) as avg_rating,
        sum(j.num_votes) as total_votes
    from joined j
    group by j.person_id, j.person_name, j.role
)

select 
    a.*,
    t.top_titles
from agg a
join top_titles t on a.person_id = t.person_id
where 
    num_titles >= 3
    and avg_rating >= 7.5
    and total_votes >= 100000
order by avg_rating desc, total_votes desc
