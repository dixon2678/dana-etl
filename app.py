
import json
import os
import glob
from google.cloud import storage
from google.oauth2 import service_account
from pathlib import Path
from pandas.io import parsers
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from google.cloud import storage
from flask import Flask, Response, request
from functions import *





# Initiate Flask app
app = Flask(__name__)

# Initiate ETL functions
usecase = etlUsecase()

# List of files we want to ETL
files = ['yelp_academic_dataset_user', 'yelp_academic_dataset_tip', 'yelp_academic_dataset_review', 'yelp_academic_dataset_checkin', 'yelp_academic_dataset_business']

# files = ['yelp_academic_dataset_tip']

# Convert JSON to CSV endpoint
@app.route("/api/convert_to_csv", methods=['GET'])
def json_to_csv():
    for dataset in files:
        usecase.append_if_exist(dataset)
    return 'Job Finished'

# Convert CSV to parquet endpoint
@app.route("/api/convert_to_parquet", methods=['GET'])
def csv_to_parquet():
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    for file in files:
        usecase.process_if_exists(file)
    sc.stop()
    return 'Job Finished'

# Load parquet files + weather CSV to GCS (staging area)
@app.route("/api/load_to_gcs", methods=['POST'])
def load_to_gcs():
    # Initiate GCP creds from 'creds' body of the request
    rq = request.get_json()
    gcp_creds = rq['creds']
    gcp_json_credentials_dict = json.loads(gcp_creds)
    credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)
    client = storage.Client(credentials=credentials, project='alpine-furnace-373410')
    bucket = client.get_bucket('dana-staging')
    # Iterate and upload parquet files
    parquet_files = glob.glob(str(cwd) + '/**.parquet/*.parquet', recursive=True)
    for file in parquet_files:
        usecase.upload_to_gcs(Path(file), bucket)
    # Upload weather data
    usecase.upload_to_gcs(Path('us_climate_philadelphia_daily.csv'), bucket)
    return 'Job Finished'



# Endpoint health
@app.route("/health")
def el_job():
    return {"Status": "OK!"}

@app.route("/")
def home():
    return 'Home'

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5777)))