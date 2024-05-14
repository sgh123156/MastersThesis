# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("ProductSubcategoryID", IntegerType(), True),
    StructField("ProductCategoryID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
