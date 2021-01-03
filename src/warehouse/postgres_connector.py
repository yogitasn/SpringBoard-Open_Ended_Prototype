from pyspark.sql import DataFrameWriter

class PostgresConnector(object):
    """
    Helper function to connect to Postgres and load dataframe
    
    """
    def __init__(self):
        #self.database_name = occupancy
        #self.hostname = localhost
        self.url_connect = "jdbc:postgresql://localhost:5432/occupancy"
        self.properties = {"user":"postgres", 
                      "password":'Quark@2416',
                      "driver": "org.postgresql.Driver"
                     }
    def get_writer(self, df):
        return DataFrameWriter(df)
        
    def write(self, df, table, mode):
        my_writer = self.get_writer(df)
        my_writer.jdbc(self.url_connect, table, mode, self.properties)