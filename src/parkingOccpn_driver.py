import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import logging.config
import configparser
import time
from warehouse.parking_warehouse_driver import ParkingWarehouseDriver
from parkingOccpn_transform_load import ParkingOccupancyLoadTransform

def main():
    """
    This method performs below tasks:
    1: Run Data Warehouse functionality by setting up Staging tables 
    2. Perform transformations on the dataset and load the staging warehouse tables 
    """
    logging.debug("\n\nSetting up Spark Session...")
    pot = ParkingOccupancyLoadTransform()

    # Modules in the project
    modules = {
        "parkingOccupancy.csv": pot.transform_load_parking_hist_occupancy,
        "blockface.csv" : pot.transform_load_blockface_dataset
    }

  
    logging.debug("Waiting before setting up Warehouse")
    time.sleep(5)

    # Starting warehouse functionality
    prwarehouse = ParkingWarehouseDriver()
    logging.debug("Setting up staging tables")
    prwarehouse.setup_staging_tables()
    logging.debug("Populating staging tables")
    for file in modules.keys():
        modules[file]()

# Entry point for the pipeline
if __name__ == "__main__":
    main()