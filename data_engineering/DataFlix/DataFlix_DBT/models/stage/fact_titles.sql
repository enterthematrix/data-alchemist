with titles as (
    select
        t.tconst as title_id,
        t.primary_title,
        t.original_title,
        t.title_type,
        t.is_adult,
        t.start_year,
        t.end_year,
        t.runtime_minutes,
        t.genres
    from {{ ref('title_basics') }} t
),

ratings as (
    select
        tconst as title_id,
        average_rating,
        num_votes
    from {{ ref('title_ratings') }}
),

crew as (
    select
        tconst as title_id,
        director_ids as directors,
        writer_ids as writers
    from {{ ref('title_crew') }}
),

director_names as (
    select
        c.title_id,
    -- get comma separated directors list
        LISTAGG(n.primary_name, ', ') as director_names
    from crew c
    -- Un-nest directors and get names
    join lateral flatten(input => split(c.directors, ',')) as d
        join {{ ref('name_basics') }} n
            on n.nconst = d.value
    group by c.title_id
),

writer_names as (
    select
        c.title_id,
        -- get comma separated writers list
        LISTAGG(n.primary_name, ', ') as writer_names
    from crew c
    -- Un-nest writers and get names
    join lateral flatten(input => split(c.writers, ',')) as w
        join {{ ref('name_basics') }} n
            on n.nconst = w.value
    group by c.title_id
)

select
    t.title_id,
    t.primary_title,
    t.original_title,
    t.title_type,
    t.is_adult,
    t.start_year,
    t.end_year,
    t.runtime_minutes,
    t.genres,
    r.average_rating,
    r.num_votes,
    d.director_names,
    w.writer_names
from titles t
left join ratings r on t.title_id = r.title_id
left join director_names d on t.title_id = d.title_id
left join writer_names w on t.title_id = w.title_id
