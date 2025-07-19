{# 
Person(actor/director/writer) summary:
- total titles
- distinct genres
- average rating
- average popularity
- debut year
- most recent title year 
#}

with participation as (
    select
        ftp.person_id,
        ftp.title_id,
        ftp.category,
        dt.start_year,
        dt.end_year,
        fr.average_rating,
        ftg.genre_id,
        dg.genre
    from {{ ref('fact_title_participation') }} ftp
    join {{ ref('dim_title') }} dt on ftp.title_id = dt.title_id
    left join {{ ref('fact_ratings') }} fr on ftp.title_id = fr.title_id
    left join {{ ref('fact_title_genre') }} ftg on ftp.title_id = ftg.title_id
    left join {{ ref('dim_genre') }} dg on ftg.genre_id = dg.genre_id
),
ranked_category as (
    -- Find top category per person
    select
        person_id,
        category,
        count(*) as category_count,
        row_number() over (partition by person_id order by category_count desc) as category_rank
    from participation
    group by person_id, category
),
top_ranked_category as (
select person_id, category from ranked_category where category_rank = 1
),
aggregated as (
    select
        p.person_id,
        min(dp.primary_name) as primary_name,
        min(trc.category) as top_category,
        count(distinct p.title_id) as total_titles,
        count(distinct genre_id) as distinct_genres,
        round(avg(p.average_rating), 2) as avg_rating,
        min(start_year) as debut_year,
        max(end_year) as last_active_year,
        --LISTAGG(distinct genre, ',') as genres
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT genre), ', ') as genres
    from participation p
    left join {{ ref('dim_person') }} dp on p.person_id = dp.person_id
    left join top_ranked_category trc on p.person_id = trc.person_id 
    group by p.person_id
)

select * from aggregated

{% if is_incremental() %}
where person_id not in (select person_id from {{ this }})
{% endif %}
