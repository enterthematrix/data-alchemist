{{ config(
    materialized='incremental',
    unique_key='title_id'
) }}

with ratings as (
    select
        tconst as title_id,
        average_rating,
        num_votes,
        ingested_at as load_ts
    from {{ ref('title_ratings') }}
    {% if is_incremental() %}
      where ingested_at > (select max(load_ts) from {{ this }})
    {% endif %}
)

select * from ratings
