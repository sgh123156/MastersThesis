# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("PersonID", IntegerType(), True),
    StructField("StoreID", IntegerType(), True),
    StructField("TerritoryID", IntegerType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
