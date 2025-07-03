from get_raw_aqi_data import daily_air_quality_job_local

if __name__ == "__main__":
    result = daily_air_quality_job_local.execute_in_process()
    print("Success:", result.success)