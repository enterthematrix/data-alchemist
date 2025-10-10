from dagster import Definitions
from .assets import aqi_dbt_assets
from .dbt_resource import dbt_resource
from .aqi_jobs import hourly_ingest_local, hourly_ingest_snowflake
from .schedules import hourly_aqi_schedule_local, hourly_aqi_schedule_snowflake,dbt_hourly_schedule

all_assets = [aqi_dbt_assets]

defs = Definitions(
    assets=all_assets,
    resources=dbt_resource,
    jobs=[
        hourly_ingest_local, 
        hourly_ingest_snowflake
        ],
    schedules=[
        hourly_aqi_schedule_local, 
        hourly_aqi_schedule_snowflake,
        dbt_hourly_schedule
        ],
)