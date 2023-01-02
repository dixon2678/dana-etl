import pandas as pd
import pyarrow.parquet as pq
import pyarrow.csv as pv
import shutil
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

# Current working directory
cwd = Path.cwd()

class etlUsecase():
    

    def append_if_exist(self, dataset_name):
        def convert_json_to_csv(json_file_path):
            # Read the JSON file in chunks, to avoid MemoryError
            # lines=True for the json file with one object per line
            chunks = pd.read_json(json_file_path, lines=True, chunksize=100000)
            filename = json_file_path.stem + '.csv'
            for i, chunk in enumerate(chunks): # Found some unusual rows on reviews, 
                chunk.dropna().to_csv(filename,
                    header=(i==0), mode='a', index=True, index_label='index')
        cwd = Path.cwd()
        json_name = dataset_name + '.json'
        csv_name = dataset_name + '.csv'
        json_file_path_user = cwd.joinpath('yelp_dataset', json_name)
        if cwd.joinpath(csv_name).is_file()==False:
            convert_json_to_csv(json_file_path_user)
        else:
            # Avoid duplicate appending to the same file
            cwd.joinpath(csv_name).unlink() # Deletes existing file, replace with updated one
            convert_json_to_csv(json_file_path_user)



    def process_if_exists(self, name):
        def spark_process_csv(name):
            csv_name = name + '.csv'
            # Initialize Spark session
            spark = SparkSession \
                .builder \
                .master("local[*]") \
                .appName("Transform Spark Session") \
                .getOrCreate()
            #spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
            df = spark.read.options(header=True,  escape='"', quotes='"', sep=',', inferSchema=True, limit=5).csv(csv_name, multiLine=True)
            #df.write.parquet('gs://dana-staging/', compression="gzip")
            # Write to parquet with GZIP compression
            df.write.parquet(name + '.parquet', compression='gzip')
        csv_name = name + '.csv'
        parquet_name = name + '.parquet'
        if cwd.joinpath(csv_name).is_file()==False:
            return '.csv file missing'
        elif cwd.joinpath(parquet_name).is_dir()==True:
            shutil.rmtree(cwd.joinpath(parquet_name))
            spark_process_csv(name)
        elif cwd.joinpath(parquet_name).is_dir()==False:
            spark_process_csv(name)


    def upload_to_gcs(self, path, bucket):
        blob = bucket.blob(path.name)
        blob.upload_from_filename(path, timeout=None)