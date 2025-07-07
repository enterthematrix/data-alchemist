from dagster import Definitions
from aqi_dag.get_raw_aqi_data import daily_air_quality_job, daily_air_quality_job_local
from aqi_dag.schedules import daily_air_quality_schedule, daily_air_quality_schedule_local

defs = Definitions(
    jobs=[
        daily_air_quality_job,
        daily_air_quality_job_local,
    ],
    schedules=[
        daily_air_quality_schedule,
        daily_air_quality_schedule_local,
    ],
)
