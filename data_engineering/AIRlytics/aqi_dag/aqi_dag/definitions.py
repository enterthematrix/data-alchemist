from dagster import Definitions
#from aqi_dag.get_raw_aqi_data import hourly_ingest_local, hourly_ingest_snowflake
from aqi_dag.aqi_jobs import hourly_ingest_local, hourly_ingest_snowflake
from aqi_dag.schedules import hourly_aqi_schedule_local, hourly_aqi_schedule_snowflake


defs = Definitions(
    jobs=[
        hourly_ingest_local, 
        hourly_ingest_snowflake
        ],
    schedules=[
        hourly_aqi_schedule_local, 
        hourly_aqi_schedule_snowflake
        ],
)
