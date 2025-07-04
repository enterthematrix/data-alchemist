# Setup: Snowflake user/role/schema creation and data load

## Snowflake setup

```sql {#snowflake_setup}
-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS ADHOC_WH;
GRANT OPERATE ON WAREHOUSE ADHOC_WH TO ROLE TRANSFORM;

-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbt123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='ADHOC_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='AQI.DEV'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM to USER dbt;

-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS AQI;
CREATE SCHEMA IF NOT EXISTS AQI.RAW;

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE ADHOC_WH TO ROLE TRANSFORM; 
GRANT ALL ON DATABASE AQI to ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE AQI to ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE AQI to ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA AQI.RAW to ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA AQI.RAW to ROLE TRANSFORM;
GRANT USAGE, READ ON STAGE AQI.RAW.AQI_RAW_DATA_STG TO ROLE TRANSFORM;
GRANT USAGE ON FILE FORMAT AQI.RAW.JSON_FILE_FORMAT TO ROLE TRANSFORM;



-- Set up the defaults
USE WAREHOUSE ADHOC_WH;
USE DATABASE AQI;
USE SCHEMA RAW;

-- create an internal stage and enable directory service
CREATE STAGE IF NOT EXISTS aqi_raw_data_stg
directory = ( enable = true)
comment = 'Internal Stage to store raw air quality data';


-- create file format to process the JSON file
CREATE FILE FORMAT IF NOT EXISTS json_file_format
type = 'JSON' compression = 'AUTO'
comment = 'JSON file format object';


SHOW STAGES;
SHOW FILE FORMATS;

-- list files under the stage
LIST @aqi_raw_data_stg;


-- Create our raw tables 

```

## Dagster setup

```
-- Install dependencies:

dbt-snowflake==1.7.1 # dagsater has issues with later version
dagster-dbt==0.22.0
dagster-webserver==1.6.0

-- Create Dagster project:

dagster-dbt project scaffold --project-name aqi_dag --dbt-project-dir=aqi

-- Run Dagster service
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev

```
