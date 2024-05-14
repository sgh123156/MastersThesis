# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("OrderKey", LongType(), True),
    StructField("CustomerKey", IntegerType(), True),
    StructField("StoreKey", IntegerType(), True),
    StructField("OrderDate", DateType(), True),
    StructField("DeliveryDate", DateType(), True),
    StructField("CurrencyCode", StringType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
