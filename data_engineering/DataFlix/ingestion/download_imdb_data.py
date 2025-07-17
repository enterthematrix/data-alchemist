import os, sys, logging
import requests
import json, yaml
from pathlib import Path
from datetime import datetime
import pytz
from snowflake.snowpark import Session
from dagster import job, op, RetryPolicy


# Create the logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)
log_filename = f"logs/imdb_ingest.log"

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

# Access account and password from the 'imdb' profile
imdb_credentials = profiles['imdb']['outputs']['dev']

SNOWFLAKE_ACCOUNT = imdb_credentials['account']
SNOWFLAKE_USER = imdb_credentials['user']
SNOWFLAKE_ROLE = imdb_credentials['role']
SNOWFLAKE_PASSWORD = imdb_credentials['password']
SNOWFLAKE_DATABASE = imdb_credentials['database']
SNOWFLAKE_SCHEMA = imdb_credentials['schema']
SNOWFLAKE_WAREHOUSE = imdb_credentials['warehouse']
SNOWFLAKE_STAGE = 'imdb_raw_data'

SNOWFLAKE_STAGE_PATH = f'@{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}/'


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

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
    return Session.builder.configs(connection_parameters).create()

# List of IMDb dataset filenames
IMDB_FILES = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz"
]

BASE_URL = "https://datasets.imdbws.com/"
DATA_DIR = Path(__file__).resolve().parents[1] / "data"

def download_file(file_name):
    url = BASE_URL + file_name
    dest = DATA_DIR / file_name

    print(f"Downloading {file_name}...")
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dest, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Saved to {dest}")
            sf_session = snowpark_basic_auth()
            
            logging.info(f'Placing the file, the file name is {file_name} and stage location is {SNOWFLAKE_STAGE_PATH}')

            # Upload file to Snowflake stage
            put_results = sf_session.file.put(str(dest), SNOWFLAKE_STAGE_PATH, overwrite=True)
            logging.info(f'PUT result: {put_results}')

            # Check PUT result
            success = all(res.status == 'UPLOADED' for res in put_results)

            if success:
                logging.info(f"File successfully uploaded to Snowflake: {file_name}")
                
                # Delete local files
                for f in [file_name]:
                    try:
                        dest.unlink()
                        logging.info(f"Deleted local file: {dest}")
                    except Exception as e:
                        logging.warning(f"Failed to delete {dest}: {e}")
            else:
                logging.error("File upload to Snowflake failed. Local files retained for debugging.") 
            
            logging.info('The ingest job complated successfully !!')

        else:
            logging.error(f"Error: {response.status_code} - {response.text}")
            sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    for file_name in IMDB_FILES:
        download_file(file_name)

if __name__ == "__main__":
    main()
