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
def spark_session_init():
    spark = SparkSession.builder\
            .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .config("spark.hadoop.fs.s3a.access.key",AWS_ACCESS_KEY_ID)\
            .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)\
            .enableHiveSupport().getOrCreate()
    return spark

def check_dim_i94addr(spark):
    
    print("=== Checking dim_i94addr table: ===")
    
    # Load table parquet files to dataframe
    dim_i94addr_dir = './ws_parquet_outputs/dim_i94addr.parquet'
    dim_i94addr_df = spark.read.parquet(dim_i94addr_dir)

    # Check row samples
    print("Row samples: ")
    dim_i94addr_df.show(5)

    # Check datatype
    print("Data types: {}".format(dim_i94addr_df.dtypes))

    # Check data row volume
    print("Amount of total rows: {}".format(dim_i94addr_df.count()))

    # Check distinct value by key
    key = ['immi_state_code']
    print("Amount of distinc rows by key: {}".format(dim_i94addr_df.select(key).distinct().count()))

    print("   Finish dim_i94addr table:    ")

    return dim_i94addr_df

def check_dim_i94port(spark):

    print("=== Checking dim_i94port table: ===")

    # Load table parquet files to dataframe
    dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
    dim_i94port_df = spark.read.parquet(dim_i94port_dir)

    # Check row samples
    print("Row samples: ")
    dim_i94port_df.show(5)

    # Check datatype
    print("Data types: {}".format(dim_i94port_df.dtypes))

    # Check data row volume
    print("Amount of total rows: {}".format(dim_i94port_df.count()))

    # Check distinct value by key
    key = ['immi_port_code']
    print("Amount of distinc rows by key: {}".format(dim_i94port_df.select(key).distinct().count()))

    print("   Finish dim_i94port table:    ")

    return dim_i94port_df

def check_dim_immi_flight(spark):

    print("=== Checking dim_immi_flight table: ===")

    # Load table parquet files to dataframe
    dim_immi_flight_dir = './ws_parquet_outputs/dim_immi_flight.parquet'
    dim_immi_flight_df = spark.read.parquet(dim_immi_flight_dir)

    # Check row samples
    print("Row samples: ")
    dim_immi_flight_df.show(5)

    # Check datatype
    print("Data types: {}".format(dim_immi_flight_df.dtypes))

    # Check data row volume
    print("Amount of total rows: {}".format(dim_immi_flight_df.count()))

    # Check distinct value by key
    key = ['flight_number']
    print("Amount of distinc rows by key: {}".format(dim_immi_flight_df.select(key).distinct().count()))

    print("   Finish dim_immi_flight table:    ")

    return dim_immi_flight_df

def check_dim_immi_travaller(spark):

    print("=== Checking dim_immi_travaller table: ===")

    # Load table parquet files to dataframe
    dim_immi_travaller_dir = './ws_parquet_outputs/dim_immi_travaller.parquet'
    dim_immi_travaller_df = spark.read.parquet(dim_immi_travaller_dir)

    # Check row samples
    print("Row samples: ")
    dim_immi_travaller_df.show(5)

    # Check datatype
    print("Data types: {}".format(dim_immi_travaller_df.dtypes))

    # Check data row volume
    print("Amount of total rows: {}".format(dim_immi_travaller_df.count()))

    # Check distinct value by key
    key = ['traveller_cicid']
    print("Amount of distinc rows by key: {}".format(dim_immi_travaller_df.select(key).distinct().count()))

    print("   Finish dim_immi_travaller table:    ")

    return dim_immi_travaller_df

def check_dim_visa(spark):

    print("=== Checking dim_visa table: ===")

    # Load table parquet files to dataframe
    dim_visa_dir = './ws_parquet_outputs/dim_visa.parquet'
    dim_visa_df = spark.read.parquet(dim_visa_dir)

    # Check row samples
    print("Row samples: ")
    dim_visa_df.show(5)

    # Check datatype
    print("Data types: {}".format(dim_visa_df.dtypes))

    # Check data row volume
    print("Amount of total rows: {}".format(dim_visa_df.count()))

    # Check distinct value by key
    key = ['visatype_by_code']
    print("Amount of distinc rows by key: {}".format(dim_visa_df.select(key).distinct().count()))

    print("   Finish dim_visa table:    ")

    return dim_visa_df

def check_fact_i94immi(spark):

    print("=== Checking fact_i94immi table: ===")

    # Load table parquet files to dataframe
    fact_i94immi_dir = './ws_parquet_outputs/fact_i94immi.parquet'
    fact_i94immi_df = spark.read.parquet(fact_i94immi_dir)

    # Check row samples
    print("Row samples: ")
    fact_i94immi_df.show(5)

    # Check datatype
    print("Data types: {}".format(fact_i94immi_df.dtypes))

    # Check data row volume
    print("Amount of total rows: {}".format(fact_i94immi_df.count()))

    # Check distinct value by key
    key = ['travel_cicid']
    print("Amount of distinc rows by key: {}".format(fact_i94immi_df.select(key).distinct().count()))

    print("   Finish fact_i94immi table:    ")

    return fact_i94immi_df

def check_fact_worldtempe(spark):

    print("=== Checking fact_worldtempe table: ===")

    # Load table parquet files to dataframe
    fact_worldtempe_dir = './ws_parquet_outputs/fact_worldtempe.parquet'
    fact_worldtempe_df = spark.read.parquet(fact_worldtempe_dir)

    # Check row samples
    print("Row samples: ")
    fact_worldtempe_df.show(5)

    # Check datatype
    print("Data types: {}".format(fact_worldtempe_df.dtypes))

    # Check data row volume
    print("Amount of total rows: {}".format(fact_worldtempe_df.count()))

    # Check distinct value by key
    key = ['measure_city', 'measure_date']
    print("Amount of distinc rows by key: {}".format(fact_worldtempe_df.select(key).distinct().count()))

    print("   Finish fact_worldtempe table:    ")

    return fact_worldtempe_df

def check_airport_traffic(spark):
    # Count air flight traffic at airport
    # Load dataframes to Spark SQL tables
    
    fact_i94immi_dir = './ws_parquet_outputs/fact_i94immi.parquet'
    fact_i94immi_df = spark.read.parquet(fact_i94immi_dir)
    fact_i94immi_df.createOrReplaceTempView('fact_i94immi')

    dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
    dim_i94port_df = spark.read.parquet(dim_i94port_dir)
    dim_i94port_df.createOrReplaceTempView('dim_i94port')

    # Create table view for airline traffic
    spark.sql("""
            SELECT 
                fact_i94.arrival_port_code as airport_code,
                dim_i94.immi_city_name as city_name,
                fact_i94.immi_flight_code as flight_traffic
            FROM fact_i94immi as fact_i94
            JOIN dim_i94port as dim_i94
                ON dim_i94.immi_port_code = fact_i94.arrival_port_code
            GROUP BY city_name, airport_code, flight_traffic
        """).createOrReplaceTempView('city_flight_traffic')
    
    spark.sql("""
        SELECT 
            COUNT(flight_traffic) as air_traffic, 
            city_name
        FROM city_fligh_traffic
        GROUP BY city_name
        ORDER BY air_traffic DESC
    """).createOrReplaceTempView('city_flight_traffic')
    
    spark.sql("""
        SELECT 
            *
        FROM city_flight_traffic
    """).show(30)

    return None

def check_immi_city(spark):
    # JOIN count(i94_immi.cicid) with city
    dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
    dim_i94port_df = spark.read.parquet(dim_i94port_dir)
    dim_i94port_df.createOrReplaceTempView('dim_i94port')

    dim_immi_travaller_dir = './ws_parquet_outputs/dim_immi_travaller.parquet'
    dim_immi_travaller_df = spark.read.parquet(dim_immi_travaller_dir)
    dim_immi_travaller_df.createOrReplaceTempView('dim_immi_travaller')

    # Create table view for immigration volume
    spark.sql("""
            SELECT 
                arrival_port_code as airport_code,
                COUNT(travel_cicid) as total_traveller
            FROM fact_i94immi
            GROUP BY airport_code
            ORDER BY total_traveller DESC
        """).createOrReplaceTempView('city_immi_volume')
    
    spark.sql("""
        SELECT 
            city_immi_vol.airport_code as airport_name,
            dim_i94port.immi_city_name as city_name,
            city_immi_vol.total_traveller as travel_volume
        FROM city_immi_volume as city_immi_vol
        JOIN dim_i94port as dim_i94port
            ON dim_i94port.immi_port_code = city_immi_vol.airport_code
    """).createOrReplaceTempView('city_immi_volume')

    spark.sql("""
        SELECT *
        FROM city_immi_volume
    """).show(30)

    return None

def main():

    return None

def main():

    # Define Spark session initilization
    spark = spark_session_init()

    

    # List of parquet files of fact & dim tables
    parquet_outputs = './ws_parquet_outputs'

    dim_i94addr_dir = './ws_parquet_outputs/dim_i94addr.parquet'
    dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
    dim_immi_flight_dir = './ws_parquet_outputs/dim_immi_flight.parquet'
    dim_immi_travaller_dir = './ws_parquet_outputs/dim_immi_travaller.parquet'
    dim_visa_dir = './ws_parquet_outputs/dim_visa.parquet'
    fact_i94immi_dir = './ws_parquet_outputs/fact_i94immi.parquet'
    fact_worldtempe_dir = './ws_parquet_outputs/fact_worldtempe.parquet'

    # Explore and garther information
    dim_immi_flight_key = ['fltno']

    # PRIMARY KEY
    # COMPOSITE KEYs includes PARTITION KEY and CLUSTERING KEY
    check_dim_i94addr(spark)
    check_dim_i94port(spark)
    check_dim_immi_flight(spark)
    check_dim_immi_travaller(spark)
    check_dim_visa(spark)
    check_fact_i94immi(spark)
    check_fact_worldtempe(spark)

    check_airport_traffic(spark)
    check_immi_city(spark)

    print ("====== QUALITY CHECK PROCESSES DONE =====")

if __name__ == "__main__":
    main()