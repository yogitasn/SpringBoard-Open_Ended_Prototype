import configparser
import logging
import psycopg2
from warehouse.staging_queries import drop_staging_tables,create_staging_tables
from pathlib import Path

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('.\\warehouse\\warehouse_config.cfg')

class ParkingWarehouseDriver:

    """
    The below method is used to create staging tables in postgres
    """

    def __init__(self):
        self._conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DATABASE'].values()))
        self._cur = self._conn.cursor()


    def setup_staging_tables(self):
    
        logging.debug("Dropping Staging tables.")
        self.execute_query(drop_staging_tables)

        logging.debug("Creating Staging tables.")
        self.execute_query(create_staging_tables)


    def execute_query(self, query_list):
        for query in query_list:
            print(query)
            logging.debug(f"Executing Query : {query}")
            self._cur.execute(query)
            self._conn.commit()