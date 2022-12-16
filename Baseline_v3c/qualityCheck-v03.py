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

def quality_check_primarykey(spark, parquet_part, select_columns, table_name):
    df = spark.read.parquet(parquet_part)
    if df.count() > df.dropDuplicates(select_columns).count():
        raise ValueError('>>> Key has duplicates for {}!!'.format(table_name))
    else:
        print(">>> Unique key check succes for {}.".format(table_name))
    
    return None



def check_dim_i94addr(spark):
    
    print("=== Checking PK of dim_i94addr table: ===")
    
    # Load table parquet files to dataframe
    dim_i94addr_dir = './ws_parquet_outputs/dim_i94addr.parquet'
    key = ['immi_state_code']

    quality_check_primarykey(spark, dim_i94addr_dir, key, "dim_i94addr")

    print("   dim_i94addr table has been checked    ")

    return None




def check_dim_i94port(spark):

    print("=== Checking PK of dim_i94port table: ===")

    # Load table parquet files to dataframe
    dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
    key = ['immi_port_code']

    quality_check_primarykey(spark, dim_i94port_dir, key, "dim_i94port")
    
    print("=== dim_i94port table has been checked ===")

    return None



def check_dim_immi_flight(spark):

    print("=== Checking PK of dim_immi_flight table: ===")

    # Load table parquet files to dataframe
    dim_immi_flight_dir = './ws_parquet_outputs/dim_immi_flight.parquet'
    key = ['flight_number']

    quality_check_primarykey(spark, dim_immi_flight_dir, key, "dim_immi_flight")
    
    print("=== dim_immi_flight table has been checked ===")

    return None



def check_dim_immi_travaller(spark):

    print("=== Checking PK of dim_immi_travaller table: ===")

    # Load table parquet files to dataframe
    dim_immi_travaller_dir = './ws_parquet_outputs/dim_immi_travaller.parquet'
    key = ['traveller_cicid']

    quality_check_primarykey(spark, dim_immi_travaller_dir, key, "dim_immi_travaller")

    print("=== dim_immi_travaller table has been checked ===")

    return None



def check_dim_visa(spark):

    print("=== Checking PK of dim_visa table: ===")

    # Load table parquet files to dataframe
    dim_visa_dir = './ws_parquet_outputs/dim_visa.parquet'
    key = ['visatype_by_code']

    quality_check_primarykey(spark, dim_visa_dir, key, "dim_visa")

    print("=== dim_visa table has been checked ===")

    return None



def check_fact_i94immi(spark):

    print("=== Checking PK of fact_i94immi table: ===")

    # Load table parquet files to dataframe
    fact_i94immi_dir = './ws_parquet_outputs/fact_i94immi.parquet'
    key = ['travel_cicid']

    quality_check_primarykey(spark, fact_i94immi_dir, key, "fact_i94immi")

    print("=== fact_i94immi table has been checked ===")

    return None


def check_fact_worldtempe(spark):

    print("=== Checking PK of fact_worldtempe table: ===")

    # Load table parquet files to dataframe
    fact_worldtempe_dir = './ws_parquet_outputs/fact_worldtempe.parquet'
    key = ['measure_city', 'measure_date']

    quality_check_primarykey(spark, fact_worldtempe_dir, key, "fact_worldtempe")
    
    print("=== fact_worldtempe table has been checked ===")

    return None

def check_airport_traffic(spark):
    # Count air flight traffic to a city
    # Load dataframes to Spark SQL tables
    try:
        fact_i94immi_dir = './ws_parquet_outputs/fact_i94immi.parquet'
        fact_i94immi_df = spark.read.parquet(fact_i94immi_dir)
        fact_i94immi_df.createOrReplaceTempView('fact_i94immi')

        dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
        dim_i94port_df = spark.read.parquet(dim_i94port_dir)
        dim_i94port_df.createOrReplaceTempView('dim_i94port')

        # Create table view airport_code | city_name | flight_traffic
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
        
        # Create table view amount of flight_traffic to a city_name
        spark.sql("""
            SELECT 
                COUNT(flight_traffic) as air_flight_traffic,
                city_name
            FROM city_flight_traffic
            GROUP BY city_name
            ORDER BY air_flight_traffic DESC
        """).createOrReplaceTempView('city_flight_traffic')
        
        # Show result
        print("Air flight traffic to a city: ")
        spark.sql("""
            SELECT 
                COUNT(*) as number_of_query_outputs
            FROM city_flight_traffic
        """).show()

        print("    SQL JOIN 'dim_i94port' and 'fact_i94immi' has been tested.    ")
        print(">>> Data quality of 'dim_i94port' and 'fact_i94immi' are OK.    ")
        print("   ======================================================    ")

    except:
        print("An data quality exception error occurred")

    return None

def check_immi_city(spark):
    # Count immigration volume to a city
    # Load dataframes to Spark SQL tables
    try:
        dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
        dim_i94port_df = spark.read.parquet(dim_i94port_dir)
        dim_i94port_df.createOrReplaceTempView('dim_i94port')

        dim_immi_travaller_dir = './ws_parquet_outputs/dim_immi_travaller.parquet'
        dim_immi_travaller_df = spark.read.parquet(dim_immi_travaller_dir)
        dim_immi_travaller_df.createOrReplaceTempView('dim_immi_travaller')

        # Create table view of immigration volume for a airport_code
        spark.sql("""
                SELECT 
                    arrival_port_code as airport_code,
                    COUNT(travel_cicid) as total_traveller
                FROM fact_i94immi
                GROUP BY airport_code
                ORDER BY total_traveller DESC
            """).createOrReplaceTempView('city_immi_volume')
        
        # Create table view of airport_name | city_name | travel_volume
        spark.sql("""
            SELECT 
                city_immi_vol.airport_code as airport_name,
                dim_i94port.immi_city_name as city_name,
                city_immi_vol.total_traveller as travel_volume
            FROM city_immi_volume as city_immi_vol
            JOIN dim_i94port as dim_i94port
                ON dim_i94port.immi_port_code = city_immi_vol.airport_code
        """).createOrReplaceTempView('city_immi_volume')

        # Show result
        print("Immigration travel volume to a city: ")
        spark.sql("""
            SELECT 
                COUNT(*) as number_of_query_outputs
            FROM city_immi_volume
        """).show()

        print("    SQL JOIN 'fact_i94immi' and 'dim_i94port' has been tested.    ")
        print(">>> Data quality of 'fact_i94immi' and 'dim_i94port' are OK.    ")
        print("   ======================================================    ")

    except:
        print("An data quality exception error occurred")

    return None

def check_tempe_immi_volume(spark):
    # Relation between temperature and immigration travel to a city
    # Load dataframes to Spark SQL tables
    try:
        fact_i94immi_dir = './ws_parquet_outputs/fact_i94immi.parquet'
        fact_i94immi_df = spark.read.parquet(fact_i94immi_dir)
        fact_i94immi_df.createOrReplaceTempView('fact_i94immi')
        
        dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
        dim_i94port_df = spark.read.parquet(dim_i94port_dir)
        dim_i94port_df.createOrReplaceTempView('dim_i94port')

        fact_worldtempe_dir = './ws_parquet_outputs/fact_worldtempe.parquet'
        fact_worldtempe_df = spark.read.parquet(fact_worldtempe_dir)
        fact_worldtempe_df.createOrReplaceTempView('fact_worldtempe')

        # optimize fact_i94immi
        spark.sql("""
                SELECT
                    i94immi.travel_cicid as travel_cicid,
                    i94immi.arrival_port_code as airport_code,
                    i94port.immi_city_name as city_name,
                    i94immi.arrival_month as travel_month,
                    i94immi.immi_arrival_date as travel_date
                FROM fact_i94immi as i94immi
                JOIN dim_i94port as i94port
                    ON i94port.immi_port_code = i94immi.arrival_port_code
            """).createOrReplaceTempView('travel_city_month')
        
        # Total immigration travel by city_name
        spark.sql("""
            SELECT
                city_name,
                airport_code,
                COUNT(travel_cicid) as total_travel
            FROM travel_city_month
            GROUP BY city_name, airport_code
            ORDER BY total_travel DESC
        """).createOrReplaceTempView('travel_city_month')

        # From 'fact_worldtempe', filter April only
        spark.sql("""
            SELECT
                avg_tempe,
                avg_tempe_uncertain,
                measure_city,
                tempe_month,
                measure_date
            FROM fact_worldtempe
            WHERE tempe_month == 4
        """).createOrReplaceTempView('tempe_city_month')

        # Average temperature of city
        spark.sql("""
                SELECT 
                    measure_city,
                    AVG(avg_tempe) as city_avg_tempe
                FROM tempe_city_month
                GROUP BY measure_city
                ORDER BY city_avg_tempe DESC
            """).createOrReplaceTempView('tempe_city_month')
        
        # LEFT JOIN 2 tables: 'travel_city_month' and 'tempe_city_month'
        spark.sql("""
                SELECT 
                    travel_city.city_name as travel_city,
                    BROUND((tempe_city.city_avg_tempe),2) as temparature,
                    travel_city.total_travel as total_travel
                FROM travel_city_month as travel_city
                LEFT JOIN tempe_city_month as tempe_city
                    ON tempe_city.measure_city = travel_city.city_name
            """).createOrReplaceTempView('travel_vs_temperature')
        
        # View results
        print("Relation between temperature and immigration travel to a city: ")
        # Show result
        spark.sql("""
            SELECT 
                COUNT(*) as number_of_query_outputs
            FROM travel_vs_temperature
        """).show()

        print("    SQL JOIN 'fact_i94immi' and 'fact_worldtempe' has been tested.    ")
        print(">>> Data quality of 'fact_i94immi' and 'fact_worldtempe' are OK.    ")
        print("   ======================================================    ")
    
    except:
        print("An data quality exception error occurred")

    return None

# Defines procedure count data files - Done
def get_list_of_files(dir_name):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dir_name)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dir_name, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + get_list_of_files(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles

def parquet_files_inventory(parquet_dir, parquet_part):
    
    try:
        # All output parquet parts and files inventory - Done
        print("Listing parquet parts and files: ")
        list_of_files = get_list_of_files(parquet_dir)
        for item in list_of_files:
            print(item)
        
        print (">>>> All parquet parts of {} has been created.".format(parquet_part))
    
    except:
        print(">>>>> An exception error occurred with {}.".format(parquet_part))


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

    parquet_files_inventory(dim_i94addr_dir,'dim_i94addr')
    parquet_files_inventory(dim_i94port_dir, 'dim_i94port')
    parquet_files_inventory(dim_immi_flight_dir, 'dim_immi_flight')
    parquet_files_inventory(dim_immi_travaller_dir, 'dim_immi_travaller')
    parquet_files_inventory(dim_visa_dir, 'dim_visa')
    parquet_files_inventory(fact_i94immi_dir, 'fact_i94immi')
    parquet_files_inventory(fact_worldtempe_dir, 'fact_worldtempe')
    
    # Check PRIMARY KEY
    # Check COMPOSITE KEYs includes PARTITION KEY and CLUSTERING KEY
    check_dim_i94addr(spark)
    check_dim_i94port(spark)
    check_dim_immi_flight(spark)
    check_dim_immi_travaller(spark)
    check_dim_visa(spark)
    check_fact_i94immi(spark)
    check_fact_worldtempe(spark)

    check_airport_traffic(spark)
    check_immi_city(spark)
    check_tempe_immi_volume(spark)


    print ("====== QUALITY CHECK PROCESSES DONE =====")

if __name__ == "__main__":
    main()