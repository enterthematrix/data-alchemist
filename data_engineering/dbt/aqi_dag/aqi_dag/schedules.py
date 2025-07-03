from dagster import ScheduleDefinition
from aqi_dag.get_raw_aqi_data import daily_air_quality_job,daily_air_quality_job_local

daily_air_quality_schedule_local = ScheduleDefinition(
    job=daily_air_quality_job_local,
    cron_schedule="0 * * * *",  # Every hour at the top of the hour
    name="daily_air_quality_schedule_local"
)

daily_air_quality_schedule = ScheduleDefinition(
    job=daily_air_quality_job,
    cron_schedule="0 * * * *",  # Every hour at the top of the hour
    name="daily_air_quality_schedule"
)