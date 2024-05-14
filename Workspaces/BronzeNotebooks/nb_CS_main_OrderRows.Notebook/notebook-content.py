# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("OrderKey", LongType(), True),
    StructField("LineNumber", IntegerType(), True),
    StructField("ProductKey", IntegerType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", DecimalType(19, 4), True),
    StructField("NetPrice", DecimalType(19, 4), True),
    StructField("UnitCost", DecimalType(19, 4), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
