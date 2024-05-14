# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("ProductModelID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("CatalogDescription", StringType(), True),
    StructField("Instructions", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
