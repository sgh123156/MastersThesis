# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("ProductNumber", StringType(), True),
    StructField("MakeFlag", BooleanType(), True),
    StructField("FinishedGoodsFlag", BooleanType(), True),
    StructField("Color", StringType(), True),
    StructField("SafetyStockLevel", ShortType(), True),
    StructField("ReorderPoint", ShortType(), True),
    StructField("StandardCost", DecimalType(19, 4), True),
    StructField("ListPrice", DecimalType(19, 4), True),
    StructField("Size", StringType(), True),
    StructField("SizeUnitMeasureCode", StringType(), True),
    StructField("WeightUnitMeasureCode", StringType(), True),
    StructField("Weight", DecimalType(8, 2), True),
    StructField("DaysToManufacture", IntegerType(), True),
    StructField("ProductLine", StringType(), True),
    StructField("Class", StringType(), True),
    StructField("Style", StringType(), True),
    StructField("ProductSubcategoryID", IntegerType(), True),
    StructField("ProductModelID", IntegerType(), True),
    StructField("SellStartDate", TimestampType(), True),
    StructField("SellEndDate", TimestampType(), True),
    StructField("DiscontinuedDate", TimestampType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
