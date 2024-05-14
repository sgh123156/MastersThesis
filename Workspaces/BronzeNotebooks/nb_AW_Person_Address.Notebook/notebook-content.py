# CELL ********************

from pyspark.sql.types import *

# CELL ********************

schema = StructType([
    StructField("AddressID", IntegerType(), True),
    StructField("AddressLine1", StringType(), True),
    StructField("AddressLine2", StringType(), True),
    StructField("City", StringType(), True),
    StructField("StateProvinceID", IntegerType(), True),
    StructField("PostalCode", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BrLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
