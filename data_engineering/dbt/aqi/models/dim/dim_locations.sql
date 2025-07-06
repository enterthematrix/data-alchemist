{{
    config(
        materialized='incremental',
        unique_key='location_pk',
    )
}}
with locations as (
    select country, state, city, station, latitude, longitude,
    hash(latitude, longitude) as location_pk
        from {{ref('clean_aqi_data')}} group by country, state, city, station, latitude, longitude
)select *
from locations
{% if is_incremental() %}
where location_pk not in (
    select location_pk from {{ this }}
)
{{ log('Loading incrementally: ' ~ this, info=True) }}
{% endif %}