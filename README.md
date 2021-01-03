## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Execution](#execution)
* [Screenshot](#screenshot)

## General Info
This project is prototype of Open Ended Data Engineering Project: Seattle Paid Parking Occupancy

## Description
A ETL pipeline is built locally using Python, Pyspark and Postgres database technologies. 
* Extraction: File extraction process is automated using Selenium Python library and headless Chrome driver.
* Transformation: After files are extracted, transformations are performed using Pyspark (Python API to support Spark)
* Loading: The transformed datasets are loaded in Postgres database


## Technologies
Project is created with:
* Postgres Database (version=13)
* Pyspark (spark3.0.1)


## Execution

Navigate to project folder and execute the following commands

* Extraction (To be executed once and saved to a local path  : 'C:\WORKING_ZONE\' as files are huge(21GB))

```
python parkingOccpn_extract.py

```

* Transformation and loading by executing the below python driver file.  Driver will create the staging tables(ParkingWarehouseDriver) and perform transformations and loading by calling ParkingOccupancyLoadTransform

```
python parkingOccpn_driver.py

```

Below are remaining python files and their functions:

* parkingOccpn_udf.py: Helper UDFS to transform certain column values in the dataset
* warehouse\staging_queries: SQL queries to create staging tables
* warehouse\parking_warehouse_driver: Driver code to call 'staging_queries' and setup the tables in postgres.
* config.cfg: Main config file to store URL, XPATH and BUCKET
* warehouse\warehouse_config.cfg: Warehouse config file to store the Postgres database properties

## Screenshot
* Final Tables loaded in Postgres

* Historical Occupancy Table

![Alt text](screenshot/OccupancyData.PNG?raw=true "Historical Occupancy Table")

* Historical Occupancy Table Record Count

![Alt text](screenshot/OccupancyRecCnt.PNG?raw=true "Historical Occupancy Table Count")

* Blockface Table

![Alt text](screenshot/BlockfaceTable.PNG?raw=true "Blockface Table")

* Blockface Table Record Count

![Alt text](screenshot/BlockfaceRecCnt.PNG?raw=true "Blockface Table Count")

* Date Dimension Table

![Alt text](screenshot/DateDim.PNG?raw=true "Date Dimension Table")

* Blockface Table Record Count

![Alt text](screenshot/OccupancyRecCnt.PNG?raw=true "Date Dimension Table Count")

