from dagster import ScheduleDefinition
from aqi_dag.aqi_jobs import hourly_ingest_local, hourly_ingest_snowflake



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