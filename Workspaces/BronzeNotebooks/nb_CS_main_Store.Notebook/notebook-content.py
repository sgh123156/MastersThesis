# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("StoreKey", IntegerType(), True),
    StructField("StoreCode", IntegerType(), True),
    StructField("Country", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("SquareMeters", IntegerType(), True),
    StructField("OpenDate", DateType(), True),
    StructField("CloseDate", DateType(), True),
    StructField("Status", StringType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
