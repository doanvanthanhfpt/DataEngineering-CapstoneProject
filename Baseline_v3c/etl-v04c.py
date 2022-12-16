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
def spark_session_init():
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
def extract_staging_sas_label(spark, label):
    """
    Process dataset 'I94_SAS_Labels_Descriptions.SAS' and output to dataframe.
    
    Steps of dataset processing:
    - Extract data from 'I94_SAS_Labels_Descriptions.SAS'.
    - Transform to dataframe.
        
    Keyword arguments:
        * label_str | label name as string datatype.

    Output:
        Return dataframe for next step.
    """
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

def describe_gather_dataset(spark):
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
    worldtempe_df.count()

    print("Garthering I94PORT dataset from 'I94_SAS_Labels_Descriptions.SAS'.")
    I94PORT_df = extract_staging_sas_label(spark, 'I94PORT')
    I94PORT_df = I94PORT_df.toPandas()
    I94PORT_df.info()
    I94PORT_df.head(5)
    I94PORT_df.count()

    print("Garthering I94ADDR dataset from 'I94_SAS_Labels_Descriptions.SAS'.")
    I94ADDR_df = extract_staging_sas_label(spark, 'I94ADDR')
    I94ADDR_df = I94ADDR_df.toPandas()
    I94ADDR_df.info()
    I94ADDR_df.head(5)
    I94ADDR_df.count()

    return None

def i94immi_dataset_cleaning(spark, i94immi_raw_dataset, output_dir):

    print("Extracting I94 Immigration dataset (from './sas_data').")
    i94immi_df = spark.read.parquet(i94immi_raw_dataset)

    # Cleaning and staging I94 Immigration dataset
    print("Transforming I94 Immigration dataset:")
    i94immi_df.createOrReplaceTempView('i94immi_table')

    # Drop amount of un-makesance records cause by `DepartureDate >= ArrivalDate`
    spark.sql("""
        SELECT *
        FROM i94immi_table
        WHERE arrdate <= depdate
    """).createOrReplaceTempView("i94immi_table")

    # Add column `arrival_date = timestone + arrdate_offset_day`, with:
    # - timestone = '1960-01-01' (***datetime*** datatype)
    # - arrdate_offset_day = 'arrdate' (***integer*** datatype)
    # - arrival_date (***datetime*** datatype)
    spark.sql("""
        SELECT *, date_add(to_date('1960-01-01'), arrdate) AS arrival_date 
        FROM i94immi_table
    """).createOrReplaceTempView("i94immi_table")

    # Add column `departure_date = timestone + depdate_offset_day`, with:
    # - `timestone` = '1960-01-01' (***datetime*** datatype)
    # - `depdate_offset_day` = 'depdate' (***integer*** datatype)
    # - `departure_date` (***datetime*** datatype)
    spark.sql("""SELECT *, CASE 
                            WHEN depdate >= arrdate THEN date_add(to_date('1960-01-01'), depdate)
                            WHEN depdate IS NULL THEN NULL
                            ELSE 'NaN' END AS departure_date 
                    FROM i94immi_table
                """).createOrReplaceTempView("i94immi_table")
    
    # Extracted i94mode from `I94_SAS_Labels_Descriptions_SAS`
    # i94mode includes:
    # {'1': 'Air', '2': 'Sea', '3': 'Land', '9': 'Not reported'}
    # Keep air arrival only, mean keep `i94mode=1`
    spark.sql("""
        SELECT *
        FROM i94immi_table
        WHERE i94mode == 1.0
    """).createOrReplaceTempView("i94immi_table")

    # Mapping `i94visa` numbers to `visatype` instead
    spark.sql("""
        SELECT *, CASE 
                    WHEN i94visa = 1.0 THEN 'Business' 
                    WHEN i94visa = 2.0 THEN 'Pleasure'
                    WHEN i94visa = 3.0 THEN 'Student'
                    ELSE 'NaN' END AS visa_type
        FROM i94immi_table
    """).createOrReplaceTempView("i94immi_table")

    # Keep user records of `male = 'M'` and `female = 'F'` only
    spark.sql("""
        SELECT * 
        FROM i94immi_table 
        WHERE gender IN ('F', 'M')
    """).createOrReplaceTempView("i94immi_table")

    # Drop NULL value on arrival state
    spark.sql("""
        SELECT *
        FROM i94immi_table
        WHERE i94addr IS NOT NULL
    """).createOrReplaceTempView("i94immi_table")

    # Keep necessary columns
    # Convert month and year timestamp
    spark.sql("""
        SELECT 
            cicid,
            i94cit,
            i94res,
            i94port,
            arrival_date,
            YEAR(arrival_date) as i94yr,
            MONTH(arrival_date) as i94mon,
            i94mode,
            i94addr,
            departure_date,
            i94bir,
            i94visa,
            count,
            dtadfile,
            biryear,
            dtaddto,
            gender,
            insnum,
            airline,
            admnum,
            fltno,
            visatype,
            visa_type
        FROM i94immi_table
    """).createOrReplaceTempView('i94immi_table')

    # Baseline the cleaned
    i94immi_df = spark.sql("""
        SELECT *
        FROM i94immi_table
    """)

    # Cleaning staging output location
    # path = './i94immi_df_clean' # Use local storage
    try:
        os.makedirs(output_dir, exist_ok = True)
        print("Directory '%s' created successfully" % output_dir)
    except OSError as error:
        print("Directory '%s' can not be created" % output_dir)

    rmdir(Path("i94immi_df_clean")) # use this line from 2nd running

    # Staging to csv file
    i94immi_df.write.options(header='True', delimiter=',').csv(output_dir)

    print("I94 US Immigration cleaning steps has been finished.")

    return None

def i94immi_dataset_handling(spark, input_dataset, output_dir):
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

    # Loading to fact & dim tables
    print("Loading I94 Immigration dataset to fact & dim tables:")
    
    # Read out from staging
    fact_i94immi_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(input_dataset)
    
    # 'fact_i94immi'
    # Transform to Spark SQL TempView
    fact_i94immi_df.createOrReplaceTempView('fact_i94immi')
    
    # Create fact table fact_i94immi
    fact_i94immi = spark.sql("""
        SELECT
            cicid as travel_cicid,
            i94cit as from_country_code,
            i94res as immi_country_code,
            i94port as arrival_port_code,
            arrival_date as immi_arrival_date,
            i94yr as arrival_year,
            i94mon as arrival_month,
            i94mode as airline_mode_code,
            i94addr as immi_state_code,
            departure_date,
            i94bir as traveller_age,
            i94visa as visatype_by_number,
            biryear as traveller_birth_year,
            gender as traveller_sex,
            fltno as immi_flight_code,
            visatype as visatype_by_code,
            visa_type
        FROM fact_i94immi
    """)

    # Save table to parquet files
    # fact_i94immi_parquet_outputs = parquet_outputs + '/fact_i94immi.parquet'
    fact_i94immi.write.mode("overwrite").parquet(output_dir)

    # fact_i94immi parquet files inventory
    print("'fact_i94immi' parquet files inventory")
    list_of_files = get_list_of_files(output_dir)
    for item in list_of_files:
        print(item)
    
    print("I94 Immigration ETL steps has been finished.")

    return None

def visa_dataset_cleaning(spark, input_dataset, output_dir):
    # Extract visa dataset from 'i94immi_raw_dataset'
    print("Extracting visa dataset from 'i94immi_cleaned_dataset'.")
    visa_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(input_dataset)

    # Keep columns of visa
    visa_df = visa_df.select(['visatype', 'i94visa', 'visa_type'])

    # Keep unique values
    visa_df = visa_df.dropDuplicates(['visatype'])

    # Cleaning staging output location
    # path = './visa_df_clean' # Use local storage
    try:
        os.makedirs(output_dir, exist_ok = True)
        print("Directory '%s' created successfully" % output_dir)
    except OSError as error:
        print("Directory '%s' can not be created" % output_dir)

    rmdir(Path(output_dir)) # use this line from 2nd running

    # Staging to csv file
    visa_df.write.options(header='True', delimiter=',').csv(output_dir)

    print("Visa dataset cleaning steps has been finished.")

    return None

def visa_dataset_handling(spark, input_dataset, output_dir):
    # 'dim_visa'
    # Reuse i94immi_df dataframe to process visa_df
    visa_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(input_dataset)

    # Transform to Spark SQL TempView
    visa_df.createOrReplaceTempView('dim_visa')

    # Create dim table
    dim_visa = spark.sql("""
        SELECT
            i94visa as visatype_by_number,
            visatype as visatype_by_code,
            visa_type as visa_category
        FROM dim_visa
        """)

    # Save table to parquet files
    # dim_visa_parquet_outputs = parquet_outputs + '/dim_visa.parquet'
    dim_visa.write.mode("overwrite").parquet(output_dir)

    # dim_visa parquet files inventory
    print("'dim_visa' parquet files inventory")
    list_of_files = get_list_of_files(output_dir)
    for item in list_of_files:
        print(item)
    
    print ("'dim_visa' parquet files has been created")

    return None

def flight_dataset_cleaning(spark, input_dataset, output_dir):
    # Extract flight dataset from 'i94immi_raw_dataset'
    print("Extracting visa dataset from 'i94immi_cleaned_dataset'.")
    flight_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(input_dataset)

    # Keep columns of visa
    flight_df = flight_df.select(['fltno', 'airline', 'i94port'])

    # Keep unique values
    flight_df = flight_df.dropDuplicates(['fltno'])

    # Cleaning staging output location
    # path = './flight_df_clean' # Use local storage
    try:
        os.makedirs(output_dir, exist_ok = True)
        print("Directory '%s' created successfully" % output_dir)
    except OSError as error:
        print("Directory '%s' can not be created" % output_dir)

    rmdir(Path(output_dir)) # use this line from 2nd running

    # Staging to csv file
    flight_df.write.options(header='True', delimiter=',').csv(output_dir)

    print("Flight dataset cleaning steps has been finished.")

    return None

def flight_dataset_handling(spark, input_dataset, output_dir):
    # 'dim_immi_flight'
    # Reuse i94immi_df dataframe to process flight_df
    flight_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(input_dataset)

    # Transform to Spark SQL TempView
    flight_df.createOrReplaceTempView('dim_immi_flight')

    # Create dim table
    dim_immi_flight = spark.sql("""
            SELECT
                fltno as flight_number,
                airline as flight_brand,
                i94port as airport_city
            FROM dim_immi_flight
            """)

    # Save table to parquet files
    # dim_immi_flight_parquet_outputs = parquet_outputs + '/dim_immi_flight.parquet'
    dim_immi_flight.write.mode("overwrite").parquet(output_dir)

    # dim_immi_flight parquet files inventory
    print("'dim_immi_flight' parquet files inventory")
    list_of_files = get_list_of_files(output_dir)
    for item in list_of_files:
        print(item)
    
    print ("'dim_immi_flight' parquet files has been created")

    return None

def traveller_dataset_cleaning(spark, input_dataset, output_dir):
    # Extract traveller dataset from 'i94immi_raw_dataset'
    print("Extracting traveller dataset (from 'i94immi_cleaned_dataset').")
    traveller_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(input_dataset)

    # Keep columns of traveller
    traveller_df = traveller_df.select(['cicid', 'i94cit', 'i94res', 'i94port', 'arrival_date', 'i94mode', 'i94addr', 'i94bir', 'biryear', 'gender', 'visatype'])

    # Keep unique values
    traveller_df = traveller_df.dropDuplicates(['cicid'])

    # Cleaning staging output location
    # path = './traveller_df_clean' # Use local storage
    try:
        os.makedirs(output_dir, exist_ok = True)
        print("Directory '%s' created successfully" % output_dir)
    except OSError as error:
        print("Directory '%s' can not be created" % output_dir)

    rmdir(Path(output_dir)) # use this line from 2nd running

    # Staging to csv file
    traveller_df.write.options(header='True', delimiter=',').csv(output_dir)

    print("Traveller dataset cleaning steps has been finished.")

    return None

def traveller_dataset_handling(spark, input_dataset, output_dir):
    # 'dim_immi_travaller'
    # Transform to Spark SQL TempView
    traveller_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(input_dataset)
    traveller_df.createOrReplaceTempView('dim_immi_travaller')

    # Create dim table
    dim_immi_travaller = spark.sql("""
        SELECT
            cicid as traveller_cicid,
            i94cit as from_country_code,
            i94res as immi_country_code,
            i94port as arrival_port_code,
            arrival_date as arrival_date,
            i94mode as airline_immi_code,
            i94addr as desination_state_code,
            i94bir as traveller_age,
            biryear as traveller_birth_year,
            gender as traveller_sex,
            visatype as visatye_by_code
        FROM dim_immi_travaller
    """)

    # Save table to parquet files
    # dim_immi_travaller_parquet_outputs = parquet_outputs + '/dim_immi_travaller.parquet'
    dim_immi_travaller.write.mode("overwrite").parquet(output_dir)

    # dim_immi_travaller parquet files inventory
    print("'dim_immi_travaller' parquet files inventory")
    list_of_files = get_list_of_files(output_dir)
    for item in list_of_files:
        print(item)
    print ("'dim_immi_travaller' parquet files has been created")

    return None

def worldtempe_dataset_cleaning(spark, worldtempe_raw_dataset, output_dir):
# def worldtempe_dataset_handling(spark,output_name):
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

    print("Extracting World Temperature dataset (from '`../../data2/GlobalLandTemperaturesByCity.csv`').")
    worldtempe_df = pd.read_csv(worldtempe_raw_dataset,sep=",")

    print("Transforming World Temperature dataset:")

    # Filter out `Country` for single value `United States` and check dataframe size
    worldtempe_df = worldtempe_df[worldtempe_df['Country']=='United States']

    # Add a column 'dt_converted' as datetime datatype
    worldtempe_df['dt_converted'] = pd.to_datetime(worldtempe_df.dt)

    # Cut off the sub-dataset before "1960-01-01"
    worldtempe_df=worldtempe_df[worldtempe_df['dt_converted']>"1960-01-01"]

    # Upper case 'City' column values
    worldtempe_df["City"] = worldtempe_df["City"].str.upper()

    # Convert column names to ***snake_case*** format. Format ex: *customer_name, billing_address, ...*
    convert_column_names(worldtempe_df)

    # Staging to csv file to convert to Spark dataframe
    worldtempe_df.to_csv('worldtempe_df_clean.csv', index=False, header=True)
    worldtempe_df = spark.read.csv("worldtempe_df_clean.csv", header=True)

    # # Change datatype of column '' as DoubleType datatype for average temperature of eache city
    worldtempe_df = worldtempe_df.withColumn("averagetemperature", worldtempe_df["averagetemperature"].cast(DoubleType()).alias("averagetemperature"))

    # Lamda function to convert string of date format to date datatype
    func =  udf (lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
    # Convert 'dt_converted' to date datatype
    worldtempe_df = worldtempe_df.withColumn('dt_converted', func(col('dt_converted')))

    # Create a tableview for Spark SQL dataframe manipulation
    worldtempe_df.createOrReplaceTempView('worldtempe_table')

    # Add columns 'tempe_month' as date and 'tempe_year' as date
    spark.sql("""
        SELECT 
            dt_converted,
            MONTH(worldtempe_table.dt_converted) as tempe_month,
            YEAR(worldtempe_table.dt_converted) as tempe_year,
            dt,
            city,
            averagetemperature,
            averagetemperatureuncertainty
        FROM worldtempe_table
    """).createOrReplaceTempView('worldtempe_table')

    # Grouping average temperature by city for city temperature consistent
    spark.sql("""
        SELECT 
            city,
            tempe_month,
            BROUND(AVG(averagetemperature),2) as averagetemperature,
            BROUND(AVG(averagetemperatureuncertainty),2) as averagetemperatureuncertainty,
            tempe_year,
            dt_converted
        FROM worldtempe_table
        GROUP BY city, tempe_month, tempe_year, dt_converted
    """).createOrReplaceTempView('worldtempe_table')

    # Baseline worldtempe_df for staging
    worldtempe_df = spark.sql("""
        SELECT *
        FROM worldtempe_table
    """)

    # Staging to csv file
    # path = './worldtempe_df_clean'
    try:
        os.makedirs(output_dir, exist_ok = True)
        print("Directory '%s' created successfully" % output_dir)
    except OSError as error:
        print("Directory '%s' can not be created" % output_dir)
        
    rmdir(Path("worldtempe_df_clean")) # use this line from 2nd running
    worldtempe_df.write.options(header='True', delimiter=',').csv(output_dir)

    print("World Temperature cleaning steps has been finished.")

    return None

def worldtempe_dataset_handling(spark, input_dataset, output_dir):
    # Read out from staging csv partitions
    fact_worldtempe_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv(input_dataset)

    # 'fact_worldtempe'
    # Transform to Spark SQL TempView
    fact_worldtempe_df.createOrReplaceTempView('fact_worldtempe')

    # Create fact table 'fact_worldtempe'
    fact_worldtempe_df = spark.sql("""
        SELECT
            averagetemperature as avg_tempe,
            averagetemperatureuncertainty as avg_tempe_uncertain,
            city as measure_city,
            tempe_month as tempe_month,
            dt_converted as dt_converted,
            dt_converted as measure_date
        FROM fact_worldtempe
    """)

    # Save table to parquet files
    print("Loading World Temperature dataset to fact & dim tables:")
    # fact_worldtempe_parquet_outputs = parquet_outputs + '/fact_worldtempe.parquet'
    fact_worldtempe_df.write.mode("overwrite").parquet(output_dir)

    # fact_worldtempe parquet files inventory
    print("'fact_worldtempe' parquet files inventory")

    list_of_files = get_list_of_files(output_dir)
    for item in list_of_files:
        print(item)

    print("World Temperature ETL steps has been finished.")

    return None

def i94port_sas_labels_handling(spark, label_str, output_dir):
    """
    Process I94PORT dataset (from 'I94_SAS_Labels_Descriptions.SAS').
    
    Steps of dataset processing:
    - Extract data I94PORT from 'I94_SAS_Labels_Descriptions.SAS'.
    - Transform data: cleaning -> staging -> output to csv for later.
    - Load to star data modeling fact & dim tables.
    
    Keyword arguments:
        * spark                 | Spark session.
        * processed_outputs     | path to output dir

    Output:
        The output of fact&dim tables saved out to parquet files in corresponding:
        * dim_i94port
    """

    print("===== ETL 'I94PORT' from 'I94_SAS_Labels_Descriptions.SAS' =====")

    I94PORT_df = extract_staging_sas_label(spark, label_str)
    I94PORT_df = I94PORT_df.toPandas()

    # clean leading and trailing white space before split column
    I94PORT_df["I94PORT_valid_code"] = I94PORT_df["I94PORT_valid_code"].str.lstrip().str.rstrip()
    I94PORT_df["I94PORT_valid_value"] = I94PORT_df["I94PORT_valid_value"].str.lstrip().str.rstrip()

    # split to port, city, state
    I94PORT_df["I94PORT_city_name"] = I94PORT_df["I94PORT_valid_value"].str.split(",").str.get(0)
    I94PORT_df["I94PORT_state_code"] = I94PORT_df["I94PORT_valid_value"].str.split(",").str.get(1)

    # clean leading and trailing white space after split column
    I94PORT_df["I94PORT_city_name"] = I94PORT_df["I94PORT_city_name"].str.lstrip().str.rstrip()
    I94PORT_df["I94PORT_state_code"] = I94PORT_df["I94PORT_state_code"].str.lstrip().str.rstrip()

    # drop missing value on I94PORT_state_code
    I94PORT_df = I94PORT_df.dropna(subset = ["I94PORT_state_code"])

    # Convert column name to snake case format
    convert_column_names(I94PORT_df)

    # Remove 'i94port_valid_value' column
    select_cols = ['i94port_valid_code', 'i94port_city_name', 'i94port_state_code']
    I94PORT_df = I94PORT_df[select_cols]
    
    # Changing to SQL Spark dataframe for staging output saving
    I94PORT_df = spark.createDataFrame(I94PORT_df)

    # Save to csv staging
    path = './i94port_staging'
    try:
        os.makedirs(path, exist_ok = True)
        print("Directory '%s' created successfully" % path)
    except OSError as error:
        print("Directory '%s' can not be created" % path)
        
    rmdir(Path("i94port_staging"))
    I94PORT_df.write.options(header='True', delimiter=',').csv("i94port_staging")

    # Read csv staging to create dim table
    I94PORT_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv("i94port_staging")

    # Changing to SQL Spark tempview to create dim table
    I94PORT_df.createOrReplaceTempView('dim_i94port')
    dim_i94port = spark.sql("""
        SELECT
            i94port_valid_code as immi_port_code,
            i94port_city_name as immi_city_name,
            i94port_state_code as immi_state_code
        FROM dim_i94port
    """)

    # dim_i94port_parquet_outputs = parquet_outputs + '/dim_i94port.parquet'
    dim_i94port.write.mode("overwrite").parquet(output_dir)

    # dim_i94port parquet files inventory
    print("'dim_i94port' parquet files inventory")
    list_of_files = get_list_of_files(output_dir)
    for item in list_of_files:
        print(item)
    
    print ("'dim_i94port' parquet files inventory finished")

    print("Loading I94PORT dataset to fact & dim tables:")

    print("I94PORT ETL steps has been finished.")

    return None

def i94addr_sas_labels_handling(spark, label_str, output_dir):
    """
    Process I94ADDR dataset (from 'I94_SAS_Labels_Descriptions.SAS').
    
    Steps of dataset processing:
    - Extract data I94ADDR from 'I94_SAS_Labels_Descriptions.SAS'.
    - Transform data: cleaning -> staging -> output to csv for later.
    - Load to star data modeling fact & dim tables.
    
    Keyword arguments:
        * spark                 | Spark session.
        * processed_outputs     | path to output dir

    Output:
        The output of fact&dim tables saved out to parquet files in corresponding:
        * dim_i94port
    """

    print("===== ETL 'I94PORT' from 'I94_SAS_Labels_Descriptions.SAS' =====")

    I94ADDR_df = extract_staging_sas_label(spark, label_str)
    I94ADDR_df = I94ADDR_df.toPandas()

    # clean leading and trailing white space before split column
    I94ADDR_df["I94ADDR_valid_code"] = I94ADDR_df["I94ADDR_valid_code"].str.lstrip().str.rstrip()
    I94ADDR_df["I94ADDR_valid_value"] = I94ADDR_df["I94ADDR_valid_value"].str.lstrip().str.rstrip()

    # Convert column name to snake case format
    convert_column_names(I94ADDR_df)
    
    # Changing to SQL Spark dataframe for staging output saving
    I94ADDR_df = spark.createDataFrame(I94ADDR_df)

    # Save to csv staging
    path = './i94addr_staging'
    try:
        os.makedirs(path, exist_ok = True)
        print("Directory '%s' created successfully" % path)
    except OSError as error:
        print("Directory '%s' can not be created" % path)
        
    rmdir(Path("i94addr_staging")) # use from 2nd running
    I94ADDR_df.write.options(header='True', delimiter=',').csv('i94addr_staging')

    # Read csv staging to create dim table
    I94ADDR_df = spark.read.options(inferSchema="true", delimiter=",", header = "true").csv("i94addr_staging")

    # Changing to SQL Spark tempview to create dim table
    I94ADDR_df.createOrReplaceTempView('dim_i94addr')
    dim_i94addr = spark.sql("""
        SELECT
            i94addr_valid_code as immi_state_code,
            i94addr_valid_value as immi_state_name
        FROM dim_i94addr
    """)

    # dim_i94addr_parquet_outputs = parquet_outputs + '/dim_i94addr.parquet'
    dim_i94addr.write.mode("overwrite").parquet(output_dir)

    # dim_i94addr parquet files inventory
    print("'dim_i94addr' parquet files inventory")
    list_of_files = get_list_of_files(output_dir)
    for item in list_of_files:
        print(item)
    print ("'dim_i94addr' parquet files inventory finished")

    print("Loading I94ADDR dataset to fact & dim tables:")

    print("I94ADDR ETL steps has been finished.")

    return None

def quality_checks():
    
    return None

def main():

    # Define Spark session initilization
    spark = spark_session_init()

    

    # Raw dataset
    i94immi_raw_dataset = './sas_data'
    worldtempe_raw_dataset = '../../data2/GlobalLandTemperaturesByCity.csv'

    sas_i94port_label = 'I94PORT' # label name from 'I94_SAS_Labels_Descriptions.SAS'
    sas_i94addr_label = 'I94ADDR' # label name from 'I94_SAS_Labels_Descriptions.SAS'

    # Cleaned dataset
    i94immi_input_dataset = "./i94immi_df_clean"
    i94immi_cleaned_dataset = "./i94immi_df_clean"
    worldtempe_cleaned_dataset = "./worldtempe_df_clean"
    
    # Output Dir definitions
    parquet_outputs = './ws_parquet_outputs'

    i94immi_cleaned_dataset = "./i94immi_df_clean"
    visa_cleaned_dataset = "./visa_df_clean"
    flight_cleaned_dataset = "./flight_df_clean"
    traveller_cleaned_dataset = "./traveller_df_clean"
    
    dim_i94addr_dir = './ws_parquet_outputs/dim_i94addr.parquet'
    dim_i94port_dir = './ws_parquet_outputs/dim_i94port.parquet'
    dim_immi_flight_dir = './ws_parquet_outputs/dim_immi_flight.parquet'
    dim_immi_travaller_dir = './ws_parquet_outputs/dim_immi_travaller.parquet'
    dim_visa_dir = './ws_parquet_outputs/dim_visa.parquet'
    fact_i94immi_dir = './ws_parquet_outputs/fact_i94immi.parquet'
    fact_worldtempe_dir = './ws_parquet_outputs/fact_worldtempe.parquet'

    # Explore and garther information - done
    describe_gather_dataset(spark)

    # Clean dataset
    i94immi_dataset_cleaning(spark, i94immi_raw_dataset, i94immi_cleaned_dataset)
    visa_dataset_cleaning(spark, i94immi_cleaned_dataset, visa_cleaned_dataset)
    flight_dataset_cleaning(spark, i94immi_cleaned_dataset, flight_cleaned_dataset)
    traveller_dataset_cleaning(spark, i94immi_cleaned_dataset, traveller_cleaned_dataset)
    worldtempe_dataset_cleaning(spark, worldtempe_raw_dataset, worldtempe_cleaned_dataset)

    # Load data processes - done
    i94immi_dataset_handling(spark, i94immi_input_dataset, fact_i94immi_dir)
    visa_dataset_handling(spark,visa_cleaned_dataset,dim_visa_dir)
    flight_dataset_handling(spark,flight_cleaned_dataset,dim_immi_flight_dir)
    traveller_dataset_handling(spark,traveller_cleaned_dataset,dim_immi_travaller_dir)
    worldtempe_dataset_handling(spark, worldtempe_cleaned_dataset, fact_worldtempe_dir)
    i94port_sas_labels_handling(spark, sas_i94port_label, dim_i94port_dir)
    i94addr_sas_labels_handling(spark, sas_i94addr_label, dim_i94addr_dir)

    # All output parquet parts and files inventory - Done
    print("All parquet parts and files: ")
    list_of_files = get_list_of_files(parquet_outputs)
    for item in list_of_files:
        print(item)
    
    print ("All parquet parts and files has been created.")

    print ("====== ETL PROCESSES DONE =====")

if __name__ == "__main__":
    main()