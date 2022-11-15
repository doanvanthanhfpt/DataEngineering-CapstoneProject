## Data Engineering Capstone Project[![](./docs/img/pin.svg)](#project-status)
A core responsibility of The National Travel and Tourism Office (NTTO) is to collect, analyze, and disseminate international travel and tourism statistics.
## Introduction[![](./docs/img/pin.svg)](#introduction)

**NTTO's** Board of Managers are charged with managing, improving, and expanding the system to fully account and report the impact of travel and tourism in the United States. The analysis results help to forcecast and operation, support make decision creates a positive climate for growth in travel and tourism by reducing institutional barriers to tourism, administers joint marketing efforts, provides official travel and tourism statistics, and coordinates efforts across federal agencies.

## Project Description[![](./docs/img/pin.svg)](#introduction)

In this project, some source datas will be use to do data modeling:
* **I94 Immigration**: The source data for I94 immigration data is available in local disk in the format of sas7bdat. This data comes from US National Tourism and Trade Office. The data dictionary is also included in this project for reference. The actual source of the data is from https://travel.trade.gov/research/reports/i94/historical/2016.html. This data is already uploaded to the workspace.

* **World Temperature Data**: This dataset came from Kaggle. This data is already uploaded to the workspace.

* **Airport Code**: This is a simple table with airport codes. The source of this data is from https://datahub.io/core/airport-codes#data. It is highly recommended to use it for educational purpose only but not for commercial or any other purpose. This data is already uploaded to the workspace.

* Other text files such as * *I94Addr.txt* *, * *I94CIT_I94RES.txt* *, * *I94Mode.txt* *, * *I94Port.txt* * and * *I94Visa.txt* * files are used to enrich immigration data for better analysis. These files are created from the * *I94_SAS_Labels_Descriptions.SAS* * file provided to describe each and every field in the immigration data.

**The project follows the follow steps**:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up
---

## Table of contents[![](./docs/img/pin.svg)](#table-of-contents)
1. [Step 1: Scope the Project and Gather Data](#step1)
   [Data Volume Assessment](#data_volume_assessment)
   [Data Attributions Assessment](#data_attributions_assessment)
   [Scope the Project](#scope_the_project)
3. [Step 2: Explore and Assess the Data](#step2)
4. [Step 3: Define the Data Model](#step3)
5. [Step 4: Run Pipelines to Model the Data](#step4)
6. [Step 5: Complete Project Write Up](step5)
   - [Multicast router](#mulitcast-router)
   - [Logging service](#logging-service)
   - [Development](#development)
7. [Final: Contribution Thanks](#thanks)
   - [Distributes solutions](#distributed-solution)
   - [Driverless devices](#driverless-devices)
   - [Real-time solutions](#real-time-solutions)
   - [Digital twin](#digital-twin)
   - [Simulation and test automations](#simulation-and-test-automations)
8. [Examples](#examples)
9. [Licensing](#licensing)
10. [Call for action](#call-for-action)

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
   
   I94 Immigration Data.
   World Temperature Data.
   U.S. City Demographic Data.
   Airport Code Table.
   
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

An example to get source codes and compile under **Linux**. You'd need at least C++17 `g++` (default) compiler. Open _Terminal_ console in your `projects` folder and take the following steps:
```shell
# Step 1: Get sources from GitHub
$ git clone https://github.com/aregtech/areg-sdk.git
$ cd areg-sdk
```
```shell
# Step 2: Compile sources from terminal by calling: make [all] [framework] [examples]
$ make all
```
After compilation, the binaries are located in `<areg-sdk>/product/build/<compiler-platform-path>/bin` folder.

_AREG SDK sources are developed for:_
<table>
  <tr>
    <td nowrap><strong>Supported OS</strong></td>
    <td>Linux (list of <a href="./docs/POSIX.md#the-list-of-posix-api-used-in-areg-sdk-including-multicast-message-router" alt="list of POSX API">POSIX API</a>), Windows 8 and higher.</td>
  </tr>
  <tr>
    <td nowrap><strong>Supported CPU</strong></td>
    <td>x86, x86_64, arm and aarch64.</td>
  </tr>
  <tr>
    <td nowrap><strong>Supported compilers</strong></td>
    <td>Version C++17 GCC, g++, clang and MSVC.</td>
  </tr>
</table>

> 💡 The other POSIX-compliant OS and compilers are not tested yet.

_Compile AREG SDK sources and examples:_
| Operating System | Quick actions to use tools and compile|
| --- | --- |
|**Linux or Windows**| Import projects in _Eclipse_ to compile with POSIX API (you may need to change Toolchain).|
|**Windows**| Open `areg-sdk.sln` file in _MS Visual Studio_ (VS2019 and higher) to compile with Win32 API.|
|**Linux**| Open gnome-terminal in Linux and call “_make_” to compile with POSIX API.|

> 💡 Compilation with _Eclipse_ under Windows might require to switch the Toolchain. For example, `Cygwin GCC`.<br>
> 💡 For Linux the default compiler is `g++`. Set prefered C++17 compiler in [conf/make/user.mk](./conf/make/user.mk) file.

Details on how to change compiler, load and compile sources for various targets are described in [HOWTO](./docs/HOWTO.md).

---

## Step 5: Complete Project Write Up[![](./docs/img/pin.svg)](#software-integration)

#### Mulitcast router

Configure [_router.init_](./framework/areg/resources/router.init) file to set the IP-address and the port of _multicast router_:
```
connection.address.tcpip    = 127.0.0.1	# the address of mcrouter host
connection.port.tcpip       = 8181      # the connection port of mcrouter
```
The multicast router forms the network and can run on any device. For example, in case of M2M communication, it can run on a gateway, in case of IPC, it can run on the same machine. In case of multithreading application development, there is no need to configure `router.init` and run _mcrouter_.

#### Logging service

Configure [_log.init_](./framework/areg/resources/log.init) to set scopes, log priorities and log file name:
```
log.file        = %home%/logs/%appname%_%time%.log # create logs in 'log' subfolder of user home 
scope.mcrouter.*= NOTSET ;                         # disable logs for mcrouter.

scope.my_app.*                   = DEBUG | SCOPE ; # enable all logs of my_app
scope.my_app.ignore_this_scope   = NOTSET ;        # disable logs of certain scopes in my_app
scope.my_app.ignore_this_group_* = NOTSET ;        # disable logs of certain scope group in my_app
```
> 💡 By default, the `router.init` and `log.init` files are located in the `config` subfolder of binaries.<br>
> 💡 To enable all logs of all applications, use `scope.*  = DEBUG | SCOPE ;` .<br>
> 💡 In the current version the logging is possible only in file.

### Development

The development guidance and step-by-step example to create a simple service-enabled application are described in [DEVELOP](./docs/DEVELOP.md).

---

## Final: Contribution Thanks[![](./docs/img/pin.svg)](#thanks)

AREG SDK can be used in a very large scope of multithreading and multiprocessing application development running on Linux or Windows machines.

#### Distributed solution

AREG SDK is a distributed computing solution, where the services can be distributed and run on any node of the network. The automatic service discovery makes service location transparent, so that the applications interact as if the components are located in one process. Developers define a _model_, which is a description of service relationship, and load it to start services during runtime. The services can easily be distributed between multiple processes.

The following is a demonstration of a static _model_ description, which is loaded to start services and unloaded to stop them.
```cpp
// main.cpp source file.

// Defines static model with 2 services
BEGIN_MODEL(NECommon::ModelName)

    BEGIN_REGISTER_THREAD( "Thread1" )
        BEGIN_REGISTER_COMPONENT( "RemoteRegistry", RemoteRegistryService )
            REGISTER_IMPLEMENT_SERVICE( NERemoteRegistry::ServiceName, NERemoteRegistry::InterfaceVersion )
        END_REGISTER_COMPONENT( "RemoteRegistry" )
    END_REGISTER_THREAD( "Thread1" )

    BEGIN_REGISTER_THREAD( "Thread2" )
        BEGIN_REGISTER_COMPONENT( "SystemShutdown", SystemShutdownService )
            REGISTER_IMPLEMENT_SERVICE( NESystemShutdown::ServiceName, NESystemShutdown::InterfaceVersion )
        END_REGISTER_COMPONENT( "SystemShutdown" )
    END_REGISTER_THREAD( "Thread2" )

END_MODEL(NECommon::ModelName)

// the main function
int main()
{
    // Initialize application, enable logging, servicing and the timer.
    Application::initApplication(true, true, true, true, nullptr, nullptr );

    // load model to start service components
    Application::loadModel(NECommon::ModelName);

    // wait until Application quit signal is set.
    Application::waitAppQuit(NECommon::WAIT_INFINITE);

    // stop and unload service components
    Application::unloadModel(NECommon::ModelName);

    // release and cleanup resources of application.
    Application::releaseApplication();

    return 0;
}
```
In the example, the `"RemoveRegistry"` and the `"SystemShudown"` are the names of components called _roles_, and the `NERemoteRegistry::ServiceName` and the `NESystemShutdown::ServiceName` are the _interface names_. In combination, they define the _service name_ used to access in the network. These MACRO create static _model_ `NECommon::ModelName`, which is loaded when call `Application::loadModel(NECommon::ModelName)`, and the services are stopped when call `Application::unloadModel(NECommon::ModelName)`.

In this example, services can be merged in one thread or distributed in 2 processes by defining a _model_ in each process. Independent on service location, neither software developers, nor service client objects feel a difference except for possible slight network latency when running IPC. The services must have unique names within the scope of visibility. Means, in case of _Public_ services, the names are unique within a network, and in case of _Local_ services, the names are unique within a process scope. An example of developing a service and a client in one and multiple processes is in [**Hello Service!**](./docs/DEVELOP.md#hello-service) project described in the development guide.

#### Driverless devices

Normally, the devices are supplied with the drivers to install in the system and with the header files to integrate in the application(s). The drivers often run in Kernel mode and the crash of the driver crashes the entire system. Driver development requires a special technique, which is different for each operating system, and it is hard to debug. 
<br><a href="/docs/img/driver-solution.png"><img src="/docs/img/driver-solution.png" alt="Kkernel-mode driver solution" style="width:70%;height:70%"/></a><br>
Our proposal is to deliver driverless service-enabled devices, where device-specific services are described in the interface prototype documents. 
<br><a href="/docs/img/driverless-solution.png"><img src="/docs/img/driverless-solution.png" alt="AREG SDK driverless solution" style="width:70%;height:70%"/></a><br>
In contrast to drivers, the service development does not differ from user mode application development, it is faster to develop, easily serves multiple applications (service clients), contains fewer risks and requires less development resources. The client object generated from the supplied service interface prototype document is easily integrated into the application to communicate and trigger device-specific service(s).

#### Real-time solutions

When a remote method of the service interface is called, the engine of AREG SDK immediately generates and delivers messages to the target component, which invokes appropriate methods of addressed service. This makes communication real-time with ultra-low networking latency. Such solutions are highly required to develop time-sensitive applications for automotive, flock of drones, medtech, real-time manufacturing, real-time monitoring and other projects.
<br><a href="/docs/img/areg-sdk-features.png"><img src="/docs/img/areg-sdk-features.png" alt="AREG SDK and multicast features" style="width:70%;height:70%"/></a><br>

#### Digital twin

Often, the digital twin applications use client-server architecture, where the middleware server collects the data of external devices and the UI application virtualizes them. In such solutions, devices interact either through server or UI client applications. The event-driven and the service-oriented architecture, and the real-time communication of AREG SDK is a perfect solution to develop digital twin applications that virtualize, monitor and control external devices, and immediately react to environment or device state change in real-time mode. External devices may also communicate without additional layers, which is an important factor for emergency, security and safety cases.

#### Simulation and test automations

When hardware provisioning to all employees is impossible, testing and checking unexpected phenomena of rapidly changing software in a simulated environment can be the most rational solution. If unit tests are used by developers to test a small portion of code and they may contain bugs, the simulation is used by developers and testers to check the system's functionality and stability. Simulations are portable and accessible to everyone, help to optimize solutions and avoid unnecessary risks. Projects using simulations are better prepared for remote work and easier to outsource.
<br><a href="/docs/img/software-layers.png"><img src="/docs/img/software-layers.png" alt="Software application 4 layers" style="width:70%;height:70%"/></a><br>
The software components in applications normally are split into Data, Controller, Business and the optional Presentation layers. Distributed and service-oriented solution of the AREG engine eases system testing in a simulated environment, where the Simulation application provides an implementation of Data layer services, so that the rest of the application can be tested without any change.

The same technique of simulating data can be used to create API-driven test automations.

---
