# Do all imports and installs here - Done
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType as R, StructField as Fld,\
    DoubleType as Dbl, StringType as Str, IntegerType as Int,\
    TimestampType as Timestamp, DateType as Date, LongType as Long
from pyspark.sql.types import DoubleType
from pyspark.sql.types import DateType
import pandas as pd
import re
import configparser
import os
import shutil
from pathlib import Path
from datetime import datetime

# Create environment variables
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
config = configparser.ConfigParser()
config.read('etl.cfg')

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']

# Create Spark session
def create_spark_session():
    spark = SparkSession.builder\
            .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .config("spark.hadoop.fs.s3a.access.key",AWS_ACCESS_KEY_ID)\
            .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)\
            .enableHiveSupport().getOrCreate()
    return spark

def describe_gather_dataset():
    """
    Garther and explore input datasets, includes:
    datatype, row samples, data volume.
    
    Input datasets:
    - I94 Immigration
    - World Temperature
    - SAS Labesl Description

    Keyword arguments:
        None
    
    Output:
        Screen of explore information.
    """

    return None


def i94immi_dataset_handling(spark, processed_outputs):
    """
    Process I94 Immigration dataset (from './sas_data').
    
    Steps of dataset processing:
    - Extract data from dataset.
    - Transform data: cleaning -> staging -> output to csv for later.
    - Load to star data modeling fact & dim tables.
    
    Keyword arguments:
        * spark                 | Spark session.
        * processed_outputs     | path to output dir

    Output:
        The output of fact&dim tables saved out to parquet files in corresponding:
        * fact_i94immi
        * dim_visa
        * dim_immi_flight
        * dim_immi_travaller
    """

    print("Extracting I94 Immigration dataset (from './sas_data').")

    print("Transforming I94 Immigration dataset:")

    print("Loading I94 Immigration dataset to fact & dim tables:")

    print("I94 Immigration ETL steps has been finished.")

    return 'error_code???'


def worldtempe_dataset_handling(spark,processed_outputs):
    """
    Process World Temperature dataset (from '../../data2/GlobalLandTemperaturesByCity.csv').
    
    Steps of dataset processing:
    - Extract data from dataset.
    - Transform data: cleaning -> staging -> output to csv for later.
    - Load to star data modeling fact & dim tables.
    
    Keyword arguments:
        * spark                 | Spark session.
        * processed_outputs     | path to output dir

    Output:
        The output of fact&dim tables saved out to parquet files in corresponding:
        * fact_worldtempe
    """

    return 'error_code???'


def sas_labels_handling(spark, label, processed_outputs):
    """
    Process World Temperature dataset (from 'I94_SAS_Labels_Descriptions.SAS').
    
    Steps of dataset processing:
    - Extract data I94PORT, I94ADDR from 'I94_SAS_Labels_Descriptions.SAS'.
    - Transform data: cleaning -> staging -> output to csv for later.
    - Load to star data modeling fact & dim tables.
    
    Keyword arguments:
        * spark                 | Spark session.
        * processed_outputs     | path to output dir

    Output:
        The output of fact&dim tables saved out to parquet files in corresponding:
        * dim_i94port
        * dim_i94addr
    """

    return 'error_code'

def quality_checks():
    
    return None

def main():



if __name__ == "__main__":
    main()