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

pip install -r requirements.txt

-- Create Dagster project:

dagster-dbt project scaffold --project-name aqi_dag --dbt-project-dir=aqi

-- Run Dagster service
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev

```


## Dagster Setup: Setting up ingest job on a Cloud VM (GCP in this case)

### Pre-Setup
```
mkdir ~/.dbt
# copy dbt project specific config
sudo vi ~/.dbt/profiles.yml

sudo mkdir /home/sanju/dagster_home
sudo chown -R sanju:sanju /home/sanju/dagster_home
chmod -R u+rwX /home/sanju/dagster_home
touch /home/sanju/dagster_home/dagster.yaml
pip install "pyarrow<19.0.0"
#verify Dagster runs
dagster-daemon run
```
### Setup Dagster service file
vi /etc/systemd/system/dagster-daemon.service

```
[Unit]
Description=Dagster Daemon Service
After=network.target

[Service]
User=sanju
WorkingDirectory=/home/sanju/workspace/data-alchemist/data_engineering/AIRlytics/aqi_dag

# Set the environment variables explicitly
Environment="PATH=/home/sanju/.pyenv/versions/3.11.10/envs/aqi/bin:/home/sanju/.pyenv/shims:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
Environment="DAGSTER_HOME=/home/sanju/dagster_home"

ExecStart=/home/sanju/.pyenv/versions/3.11.10/envs/aqi/bin/dagster-daemon run
Restart=always
RestartSec=10
Environment=PYENV_ROOT=/home/sanju/.pyenv
Environment=PATH=/home/sanju/.pyenv/versions/3.11.10/envs/your_venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
Environment="AQI_API_KEY=<key>"

[Install]
WantedBy=multi-user.target
```

```
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable dagster-daemon
sudo systemctl start dagster-daemon
sudo systemctl status dagster-daemon
```  