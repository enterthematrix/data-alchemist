import os, sys
from dagster import job, op
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from .aqi_jobs import hourly_ingest_local, hourly_ingest_snowflake

def test_daily_air_quality_job():
    result = hourly_ingest_local.execute_in_process()
    print("daily_air_quality_job_local Success:", result.success)


if __name__ == "__main__":
    test_daily_air_quality_job()

