# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("TerritoryID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("CountryRegionCode", StringType(), True),
    StructField("Group", StringType(), True),
    StructField("SalesYTD", DecimalType(19, 4), True),
    StructField("SalesLastYear", DecimalType(19, 4), True),
    StructField("CostYTD", DecimalType(19, 4), True),
    StructField("CostLastYear", DecimalType(19, 4), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
