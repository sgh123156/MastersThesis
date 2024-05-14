# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("ProductKey", IntegerType(), True),
    StructField("ProductCode", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Manufacturer", StringType(), True),
    StructField("Brand", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("WeightUnitMeasure", StringType(), True),
    StructField("Weight", DoubleType(), True),
    StructField("UnitCost", DecimalType(19, 4), True),
    StructField("UnitPrice", DecimalType(19, 4), True),
    StructField("SubcategoryCode", StringType(), True),
    StructField("Subcategory", StringType(), True),
    StructField("CategoryCode", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
