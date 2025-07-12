import os, sys
from dagster import job, op
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from aqi_dag.get_raw_aqi_data import hourly_ingest_local

def test_daily_air_quality_job():
    result = hourly_ingest_local.execute_in_process()
    print("daily_air_quality_job_local Success:", result.success)


if __name__ == "__main__":
    test_daily_air_quality_job()

