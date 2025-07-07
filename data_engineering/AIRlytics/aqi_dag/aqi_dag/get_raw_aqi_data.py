import os, sys, gzip, shutil, logging
import requests
import json, yaml
from datetime import datetime
import pytz
from snowflake.snowpark import Session
from dagster import job, op, RetryPolicy

# Create the logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)
log_filename = f"logs/aqi_ingest.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'

# DBT profiles file
profile_path = os.path.expanduser("~/.dbt/profiles.yml")

# Load the YAML file
with open(profile_path, 'r') as f:
    profiles = yaml.safe_load(f)

# Access account and password from the 'aqi' profile
aqi_credentials = profiles['aqi']['outputs']['dev']

SNOWFLAKE_ACCOUNT = aqi_credentials['account']
SNOWFLAKE_USER = aqi_credentials['user']
SNOWFLAKE_ROLE = aqi_credentials['role']
SNOWFLAKE_PASSWORD = aqi_credentials['password']
SNOWFLAKE_DATABASE = aqi_credentials['database']
#SNOWFLAKE_SCHEMA = aqi_credentials['schema']
SNOWFLAKE_SCHEMA = 'RAW'
SNOWFLAKE_WAREHOUSE = aqi_credentials['warehouse']
SNOWFLAKE_STAGE = 'aqi_raw_data_stg'

SNOWFLAKE_STAGE_PATH = f'@{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}/India/'



# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# Set the time zone
ist_timezone = pytz.timezone('Asia/Kolkata')
#nz_timezone = pytz.timezone('Pacific/Auckland')

# Get the current time in IST
current_time_ist = datetime.now(ist_timezone)

# Format the timestamp
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')

# Create the file name
file_name = f'air_quality_data_{timestamp}.json'

today_string = current_time_ist.strftime('%Y_%m_%d')

# Following credential has to come using secret whie running in automated way
def snowpark_basic_auth() -> Session:
    connection_parameters = {
        "ACCOUNT": SNOWFLAKE_ACCOUNT,
        "USER": SNOWFLAKE_USER,
        "PASSWORD": SNOWFLAKE_PASSWORD,
        "ROLE": SNOWFLAKE_ROLE,
        "DATABASE": SNOWFLAKE_DATABASE,
        "SCHEMA": SNOWFLAKE_SCHEMA,
        "WAREHOUSE": SNOWFLAKE_WAREHOUSE
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()

# Define the Dagster op to get air quality data locally
# This will be used for local testing and development
@op(
    retry_policy=RetryPolicy(max_retries=3, delay=60)  # retry up to 3 times with 60 sec delay
)
def get_air_quality_data_locally():
    # Setup
    aqi_api_key = os.getenv("AQI_API_KEY")
    limit_value = 4000

    params = {
        'api-key': aqi_api_key,
        'format': 'json',
        'limit': limit_value
    }
    headers = {
        'accept': 'application/json'
    }

    # Create data directory if it doesn't exist
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    os.makedirs(data_dir, exist_ok=True)
    daily_dir = os.path.join(data_dir, today_string)
    os.makedirs(daily_dir, exist_ok=True)

    # Generate file names
    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    file_name = os.path.join(daily_dir, f"air_quality_data_{timestamp}.json")
    compressed_file_name = file_name + '.gz'

    try:
        response = requests.get(api_url, params=params, headers=headers)
        logging.info('Got the response, check if 200 or not')

        if response.status_code == 200:
            logging.info('Got the JSON Data')
            json_data = response.json()

            logging.info('Writing the JSON file to local data directory')
            with open(file_name, 'w') as json_file:
                json.dump(json_data, json_file, indent=2)

            logging.info(f'File written: {file_name}')

            logging.info('Compressing the JSON file')
            with open(file_name, 'rb') as f_in:
                with gzip.open(compressed_file_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            assert os.path.getsize(compressed_file_name) > 0, f"{compressed_file_name} is empty!"
            logging.info(f'Compressed file created: {compressed_file_name}')

            # Delete the uncompressed file
            try:
                os.remove(file_name)
                logging.info(f'Deleted uncompressed file: {file_name}')
            except Exception as e:
                logging.warning(f"Failed to delete uncompressed file: {e}")

            logging.info('Job completed successfully')

        else:
            logging.error(f"Error: {response.status_code} - {response.text}")
            sys.exit(1)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
 
# Define the Dagster op to get air quality data and upload to Snowflake stage
# This will be used for production runs
@op(
    retry_policy=RetryPolicy(max_retries=3, delay=60)  # retry up to 3 times with 60 sec delay
)
def get_air_quality_data():

    # Get the API key
    aqi_api_key = os.getenv("AQI_API_KEY")
    limit_value = 4000

    # Parameters for the API request
    params = {
        'api-key': aqi_api_key,
        'format': 'json',
        'limit': limit_value
    }

    # Headers for the API request
    headers = {
        'accept': 'application/json'
    }

    try:
        # Make the GET request
        response = requests.get(api_url, params=params, headers=headers)

        logging.info('Got the response, check if 200 or not')
        # Check if the request was successful (status code 200)
        if response.status_code == 200:

            logging.info('Got the JSON Data')
            # Parse the JSON data from the response
            json_data = response.json()


            logging.info('Writing the JSON file into local location before it moved to snowflake stage')
            # Save the JSON data to a file
            with open(file_name, 'w') as json_file:
                json.dump(json_data, json_file, indent=2)

            logging.info(f'File Written to local disk with name: {file_name}')
            
            # Compress the file
            compressed_file_name = file_name + '.gz'
            with open(file_name, 'rb') as f_in:
                with gzip.open(compressed_file_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            assert os.path.getsize(compressed_file_name) > 0, f"{compressed_file_name} is empty!"

            stg_location = f'{SNOWFLAKE_STAGE_PATH}'+today_string+'/'
            print(f'Stage location is {stg_location}')
            sf_session = snowpark_basic_auth()
            
            logging.info(f'Placing the file, the file name is {file_name} and stage location is {stg_location}')

            # Upload file to Snowflake stage
            put_results = sf_session.file.put(compressed_file_name, stg_location, overwrite=True)
            logging.info(f'PUT result: {put_results}')

            # Check PUT result
            success = all(res.status == 'UPLOADED' for res in put_results)

            if success:
                logging.info(f"File successfully uploaded to Snowflake: {compressed_file_name}")
                
                # Delete local files
                for f in [file_name, compressed_file_name]:
                    try:
                        os.remove(f)
                        logging.info(f"Deleted local file: {f}")
                    except Exception as e:
                        logging.warning(f"Failed to delete {f}: {e}")
            else:
                logging.error("File upload to Snowflake failed. Local files retained for debugging.") 
            
            logging.info('The ingest job complated successfully !!')

        else:
            logging.error(f"Error: {response.status_code} - {response.text}")
            sys.exit(1)

    except Exception as e:
        # Handle exceptions, if any
        logging.error(f"An error occurred: {e}")
        sys.exit(1)

# Define the Dagster production job
@job
def daily_air_quality_job():
    get_air_quality_data()

# Define the Dagster test job
@job
def daily_air_quality_job_local():
    get_air_quality_data_locally()