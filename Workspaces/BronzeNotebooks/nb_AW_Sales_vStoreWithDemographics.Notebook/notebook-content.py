# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("BusinessEntityID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("AnnualSales", DecimalType(19, 4), True),
    StructField("AnnualRevenue", DecimalType(19, 4), True),
    StructField("BankName", StringType(), True),
    StructField("BusinessType", StringType(), True),
    StructField("YearOpened", IntegerType(), True),
    StructField("Specialty", StringType(), True),
    StructField("SquareFeet", IntegerType(), True),
    StructField("Brands", StringType(), True),
    StructField("Internet", StringType(), True),
    StructField("NumberEmployees", IntegerType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())

