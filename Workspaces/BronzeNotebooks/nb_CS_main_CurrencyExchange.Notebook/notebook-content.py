# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("Date", DateType(), True),
    StructField("FromCurrency", StringType(), True),
    StructField("ToCurrency", StringType(), True),
    StructField("Exchange", DoubleType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
