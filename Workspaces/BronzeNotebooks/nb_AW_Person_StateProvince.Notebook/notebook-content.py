# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("StateProvinceID", IntegerType(), True),
    StructField("StateProvinceCode", StringType(), True),
    StructField("CountryRegionCode", StringType(), True),
    StructField("IsOnlyStateProvinceFlag", BooleanType(), True),
    StructField("Name", StringType(), True),
    StructField("TerritoryID", IntegerType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("LandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
