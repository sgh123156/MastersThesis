# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("CustomerKey", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("GivenName", StringType(), True),
    StructField("MiddleInitial", StringType(), True),
    StructField("Surname", StringType(), True),
    StructField("StreetAddress", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("StateFull", StringType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("CountryFull", StringType(), True),
    StructField("Birthday", TimestampType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Occupation", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("Vehicle", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Continent", StringType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
