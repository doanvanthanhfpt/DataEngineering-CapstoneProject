## Data Engineering Capstone Project[![](./docs/img/pin.svg)](#project-status)
A core responsibility of The National Travel and Tourism Office (NTTO) is to collect, analyze, and disseminate international travel and tourism statistics.
## Introduction[![](./docs/img/pin.svg)](#introduction)

**NTTO's** Board of Managers are charged with managing, improving, and expanding the system to fully account and report the impact of travel and tourism in the United States. The analysis results help to forcecast and operation, support make decision creates a positive climate for growth in travel and tourism by reducing institutional barriers to tourism, administers joint marketing efforts, provides official travel and tourism statistics, and coordinates efforts across federal agencies.

## Project Description[![](./docs/img/pin.svg)](#introduction)

In this project, some source datas will be use to do data modeling:
* **I94 Immigration**: The source data for I94 immigration data is available in local disk in the format of sas7bdat. This data comes from US National Tourism and Trade Office. The data dictionary is also included in this project for reference. The actual source of the data is from https://travel.trade.gov/research/reports/i94/historical/2016.html. This data is already uploaded to the workspace.

* **World Temperature Data**: This dataset came from Kaggle. This data is already uploaded to the workspace.

* **Airport Code**: This is a simple table with airport codes. The source of this data is from https://datahub.io/core/airport-codes#data. It is highly recommended to use it for educational purpose only but not for commercial or any other purpose. This data is already uploaded to the workspace.

* Other text files such as **validated_i94port.txt**, **validated_i94_visa.txt**, **validated_ i94addr.txt**, **validated_i94_state.txt**, **validated_i94_model.txt** and **raw_i94_country.txt** files are used to enrich immigration data for better analysis. These files are created from the **I94_SAS_Labels_Descriptions.SAS** file provided to describe each and every field in the immigration data.

**The project follows the follow steps**:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up
---

## Table of contents[![](./docs/img/pin.svg)](#table-of-contents)
1. [Step 1: Scope the Project and Gather Data](#step1)

   [Data Volume Assessment](#data_volume_assessment)<br>
   [Data Attributions Assessment](#data_attributions_assessment)<br>
   [Scope the Project](#scope_the_project)<br>

2. [Step 2: Explore and Assess the Data](#step2)

3. [Step 3: Define the Data Model](#step3)

4. [Step 4: Run Pipelines to Model the Data](#step4)

5. [Step 5: Complete Project Write Up](step5)

---

## Step 1: Scope the Project and Gather Data[![](./docs/img/pin.svg)](#step1)

#### Data Assessment[![](./docs/img/pin.svg)](#data_volume_assessment)

* Create confg file [_etl.cfg_](./etl.cfg) to set basic data path configurations:
   ```
   [DIR]
   INPUT_DIR = .
   OUTPUT_DIR = ./storage

   [DATA]
   I94_IMMI = ../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat
   WORLD_TEMPE = ../../data2/GlobalLandTemperaturesByCity.csv
   CITY_DEMOGRAPHIC = ./us-cities-demographics.csv
   AIR_PORT = ./airport-codes_csv.csv

   [SPLIT]
   I94_IMMI_SPLITED_DIR = ./storage/.sas7bdat
   WORLD_TEMPE_SPLITED_DIR = ./storage/.csv
   ```
   > 💡 The config file can be update up to steps.<br>
   > 💡 Use comment symbols: "#" or ";".<br>

* Volume asessments:<br>
   - To get date size, amount of records and column names with corresponding data type of I94 Immigration Data, run [_i94_immigration_data_gathering.py_](./i94_immigration_data_gathering.py) at terminal or Jupiter Note Book.<br>
   ```
   I94 Immigration Data
   data size here
   number of rows here
   column names with corresponding data type
   ```
   - To get date size, amount of records and column names with corresponding data type of World Temperature Data, run  [_world_temperature_data_gathering.py_](./world_temperature_data_gathering.py) at terminal or Jupiter Note Book.<br>
   ```
   World Temperature Data
   data size here
   number of rows here
   column names with corresponding data type
   ```
   - To get date size, amount of records and column names with corresponding data type of U.S. City Demographic Data, run  [_us_city_demographic_data_gathering.py_](./us_city_demographic_data_gathering.py) at terminal or Jupiter Note Book.
   ```
   U.S. City Demographic Data
   data size here
   number of rows here
   column names with corresponding data type
   ```
   - To get date size, amount of records and column names with corresponding data type of Airport Code Table, run  [_us_airport_code_table_gathering.py_](./us_airport_code_table_gathering.py) at terminal or Jupiter Note Book.
   ```
   Airport Code Table
   data size here
   number of rows here
   column names with corresponding data type</br>
   ```

* Splited data to avoid system stress and saving time for results:<br>

   To split I94 Immigration Data, run [_i94_immigration_data_gathering.py_](./i94_immigration_data_gathering.py) at terminal or Jupiter Note Book.
   ```
   I94 Immigration Data
   Path to splited files (chunks)
   List of chunks
   Amount of (chunks)
   ```
   To split World Temperature Data, run  [_world_temperature_data_gathering.py_](./world_temperature_data_gathering.py) at terminal or Jupiter Note Book.
   ```
   World Temperature Data
   Path to splited files (chunks)
   List of chunks
   Amount of (chunks)
   ```
   To split U.S. City Demographic Data, run  [_us_city_demographic_data_gathering.py_](./us_city_demographic_data_gathering.py) at terminal or Jupiter Note Book.
   ```
   U.S. City Demographic Data
   Path to splited files (chunks)
   List of chunks
   Amount of (chunks)
   ```
   To split Airport Code Table, run  [_us_airport_code_table_gathering.py_](./us_airport_code_table_gathering.py) at terminal or Jupiter Note Book.
   ```
   Airport Code Table
   Path to splited files (chunks)
   List of chunks
   Amount of (chunks)
   ```

* Attribution asessments on splited datasets:<br>

   To get schema and column names with corresponding data type of I94 Immigration Data, run [_i94_immigration_data_gathering.py_](./i94_immigration_data_gathering.py) at terminal or Jupiter Note Book.
   ```
   I94 Immigration Data
   schema included column names with corresponding data type
   sample records
   ```
   To get schema and column names with corresponding data type of World Temperature Data, run  [_world_temperature_data_gathering.py_](./world_temperature_data_gathering.py) at terminal or Jupiter Note Book.
   ```
   World Temperature Data
   schema included column names with corresponding data type
   sample records
   ```
   To get schema and column names with corresponding data type of U.S. City Demographic Data, run  [_us_city_demographic_data_gathering.py_](./us_city_demographic_data_gathering.py) at terminal or Jupiter Note Book.
   ```
   U.S. City Demographic Data
   schema included column names with corresponding data type
   sample records
   ```
   To get schema and column names with corresponding data type of Airport Code Table, run  [_us_airport_code_table_gathering.py_](./us_airport_code_table_gathering.py) at terminal or Jupiter Note Book.
   ```
   Airport Code Table
   schema included column names with corresponding data type
   sample records
   ```

### Scope the Project[![](./docs/img/pin.svg)](#scope_the_project)

* Scope datasources
   
   - I94 Immigration Data.
   - World Temperature Data.
   - U.S. City Demographic Data.
   - Airport Code Table.
   
* Analysis target

---

## Step 2: Explore and Assess the Data[![](./docs/img/pin.svg)](#step2)

* Data quality validation issues

* Clean data and staging

---

## Step 3: Define the Data Model[![](./docs/img/pin.svg)](#composition)

* Conceptual data modeling
* Data pipeline

---

## Step 4: Run Pipelines to Model the Data[![](./docs/img/pin.svg)](#software-build)

* Create the data model
* Data quality check
* Data dictionary

---

## Step 5: Complete Project Write Up[![](./docs/img/pin.svg)](#software-integration)

> 💡 By default, the `router.init` and `log.init` files are located in the `config` subfolder of binaries.<br>
> 💡 To enable all logs of all applications, use `scope.*  = DEBUG | SCOPE ;` .<br>
> 💡 In the current version the logging is possible only in file.

### Development

The development guidance and step-by-step example to create a simple service-enabled application are described in [DEVELOP](./docs/DEVELOP.md).

---

## Appreciated[![](./docs/img/pin.svg)](#thanks)

   Thanks to Teachers.<br>
   Thanks to Mentors.<br>


---
