%pyspark

import os


DW_HOST = os.environ.get("DW_HOST")
DW_PORT = os.environ.get("DW_PORT")
DW_DATABASE = os.environ.get("DW_DATABASE")
DW_USER = os.environ.get("DW_USER")
DW_PASSWORD = os.environ.get("DW_PASSWORD")


JDBC_CONNECTION_STRING_TEMPLATE = 'jdbc:postgresql://{}:{}/{}?user={}&password={}'
