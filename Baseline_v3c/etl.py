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
def spark_session_initialize():
    spark = SparkSession.builder\
            .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .config("spark.hadoop.fs.s3a.access.key",AWS_ACCESS_KEY_ID)\
            .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)\
            .enableHiveSupport().getOrCreate()
    return spark

# Procedure read out validation pair values from SAS Labels Description
def get_validation_code_from_SAS_labels(sas_input_label):
    '''
    This procedure read a input SAS Labels Description and then write out validation code datasets.
    The SAS Labels Description included validation code datasets with labels: I94RES (same to I94CIT), I94PORT, I94ADDR, I94MODE, I94VISA.
    
    Parameters
    ----------
    sas_input_label : string
        The label name of validation code dataset. Its can be one of I94RES (same to I94CIT), I94PORT, I94ADDR, I94MODE, I94VISA.
    
    Returns
    -------
    validation_code_list : validation_value_pairs(tuple(str_valid_code, str_valid_value))
        The return output is a specific SAS label list of validation code value pairs.
    '''

    # Read input SAS Labels Descriptions
    with open('I94_SAS_Labels_Descriptions.SAS') as sas_validation_code:
            labels_from_sas = sas_validation_code.read()

    # Parse labels from SAS Label Description input
    sas_labels = labels_from_sas[labels_from_sas.index(sas_input_label):]
    sas_labels = sas_labels[:sas_labels.index(';')]
    
    # Processing line by line, remove separate charaters and then append value pair
    lines = sas_labels.splitlines()
    validation_code_list = []
    for line in lines:
        try:
            valid_code, valid_value = line.split('=')
            valid_code = valid_code.strip().strip("'").strip('"')
            valid_value = valid_value.strip().strip("'").strip('"').strip()
            validation_code_list.append((valid_code, valid_value))
        except:
            pass
        
    return validation_code_list

# Procedure extract parts from SAS Labels Description
def extract_staging_sas_label(label):
    '''
    This procedure get a specific part of data dictionary from SAS_Labels_Descriptions.SAS to a schema.
    The dictionary part include valid_code and valid_value
    
    Parameters
    ----------
    label: 
        a string input of specific label from "SAS_Label_Descriptions.SAS"
        
    Syntax note: 
        input value in string datatype, need inside a pair of single quotes. Ex: 'I94RES', 'I94PORTS'
    
    Returns
    -------
    Dir of csv files with a specific part as input label.
    '''
    label_name = label
    valid_code = label + "_valid_code"
    valid_value = label + "_valid_value"
    csv_output = label + "_sas_label_validation"
    
    # Create dir for output
    parent_dir = "./"
    path = os.path.join(parent_dir, csv_output)
    try:
        os.makedirs(path, exist_ok = True)
        print("Directory '%s' created successfully" % csv_output)
    except OSError as error:
        print("Directory '%s' can not be created" % csv_output)

    # Define output dataframe structure
    schema = R([
        Fld(valid_code, Str()),
        Fld(valid_value, Str())
    ])

    # Create dataframe from extracted label
    df = spark.createDataFrame(
        data=get_validation_code_from_SAS_labels(label_name),
        schema=schema
    )

    # df.write.mode('overwrite').csv(csv_output)
    shutil.rmtree(csv_output, ignore_errors=False, onerror=None)
    df.write.options(header='True', delimiter=',').csv(csv_output)

    df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(csv_output)

    print("Top 20 rows of {} ".format(csv_output))
    df.show()

    print("Count rows of {}: {} ".format(csv_output, df.count()))
    
    print("Check unique value of {}: {} ".format(csv_output, df.select(valid_code).distinct().count()))

    print("Staging csv files in: {}".format(csv_output))

    return df

# Procedure convert column name
def convert_column_names(df):
    '''
    This procedure standardizing column names to snake case format. 
    Format ex: customer_name, billing_address, total_price.
    
    Parameters
    ----------
    dataframe : string_of_dataframe
        The input dataframe with column names might have elements of:
            - Messy columns names
            - Including accents
            - Different delimiters
            - Casing and multiple white spaces.
        Snake case style replaces the white spaces and symbol delimiters
          with underscore and converts all characters to lower case.
    
    Returns
    -------
    Dataframe with column names has been changed to snake_case format.
    '''
    cols = df.columns
    column_name_changed = []

    for col in cols:
        new_column = col.lstrip().rstrip().lower().replace (" ", "_").replace ("-", "_")
        column_name_changed.append(new_column)

    df.columns = column_name_changed

# Procedure remove specific dir (if need)
def rmdir(directory):
    '''
    This procedure perform pure recursive a directory.
    
    Parameters
    ----------
    directory : string_of_path_to_dir
        The input directory is a path to target dir. 
        This dir and all its belong child objects wil be deleted.

        Syntax note: rmdir(Path("target_path_to_dir"))
            with Path("target_path_to_dir") returns path to dir format as 'directory' input
    
    Returns
    -------
    None
    '''
    directory = Path(directory)
    for item in directory.iterdir():
        if item.is_dir():
            rmdir(item)
        else:
            item.unlink()
    directory.rmdir()


def describe_gather_dataset():
    """
    Garther and explore input datasets, includes:
    datatype, row samples, data volume.
    
    Input datasets:
    * I94 Immigration
    * World Temperature
    * SAS Labesl Description

    Keyword arguments:
        None
    
    Output:
        Screen prints explored information.
    """

    print("Garthering I94 Immigration dataset from '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'.")
    i94immi_dataset = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    i94_immi_df = spark.read.format('com.github.saurfang.sas.spark').load(i94immi_dataset)
    i94_immi_df.show(3)
    i94_immi_df.count()

    print("Garthering World Temperature dataset from '../../data2/GlobalLandTemperaturesByCity.csv'.")
    worldtempe_dataset = '../../data2/GlobalLandTemperaturesByCity.csv'
    worldtempe_df = pd.read_csv(worldtempe_dataset,sep=",")
    worldtempe_df.head(3)
    worldtempe_df.shape

    print("Garthering I94PORT dataset from 'I94_SAS_Labels_Descriptions.SAS'.")
    I94PORT_df = extract_staging_sas_label('I94PORT')
    I94PORT_df = I94PORT_df.toPandas()
    I94PORT_df.info()
    I94PORT_df.head(5)
    I94PORT_df.count()

    print("Garthering I94ADDR dataset from 'I94_SAS_Labels_Descriptions.SAS'.")
    I94ADDR_df = extract_staging_sas_label('I94ADDR')
    I94ADDR_df = I94ADDR_df.toPandas()
    I94ADDR_df.info()
    I94ADDR_df.head(5)
    I94ADDR_df.count()

    return None

def i94immi_dataset_handling(spark, output_name):
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


def worldtempe_dataset_handling(spark,output_name):
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

    print("Extracting World Temperature dataset (from './sas_data').")

    print("Transforming World Temperature dataset:")

    print("Loading World Temperature dataset to fact & dim tables:")

    print("World Temperature ETL steps has been finished.")

    return 'error_code???'


def sas_labels_handling(spark, label):
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

    print("Label being processing here.")
    
    return None

def quality_checks():
    
    return None

def main():

    spark = spark_session_initialize()

    describe_gather_dataset()

    i94immi_dataset_handling(spark, output_name)
    worldtempe_dataset_handling(spark,output_name)
    sas_labels_handling(spark, 'I94PORT')
    sas_labels_handling(spark, 'I94ADDR')



if __name__ == "__main__":
    main()