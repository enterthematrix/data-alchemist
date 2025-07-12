from dagster import job
from .get_raw_aqi_data import hourly_ingest_to_local, hourly_ingest_to_snowflake
from .hooks import notify_on_failure

@job(hooks={notify_on_failure})
def hourly_ingest_local():
    hourly_ingest_to_local()

@job(hooks={notify_on_failure})
def hourly_ingest_snowflake():
    hourly_ingest_to_snowflake()
