{{ config(
    materialized='table'
) 
}}
with l as (
    select * from {{ ref("src_listings") }}
),
h as (
    select * from {{ ref("src_hosts") }}
)
select 
l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price_st,
    l.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost,
    l.created_at,
    GREATEST(l.updated_at, h.updated_at) as updated_at
from l
join h on l.host_id = h.host_id