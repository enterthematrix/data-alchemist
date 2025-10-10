from dagster import ScheduleDefinition, define_asset_job
from .aqi_jobs import hourly_ingest_local, hourly_ingest_snowflake


dbt_build_job = define_asset_job(name="dbt_hourly_build", selection="*")

hourly_aqi_schedule_local = ScheduleDefinition(
    job=hourly_ingest_local,
    cron_schedule="0 * * * *",  # Every hour at the top of the hour
    name="daily_air_quality_schedule_local"
)

hourly_aqi_schedule_snowflake = ScheduleDefinition(
    job=hourly_ingest_snowflake,
    cron_schedule="0 * * * *",  # Every hour at the top of the hour
    name="daily_air_quality_schedule"
)

dbt_hourly_schedule = ScheduleDefinition(
    job=dbt_build_job,
    cron_schedule="5 * * * *",  # run 5 min past every hour
    name="dbt_hourly_schedule"
)