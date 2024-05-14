# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("BusinessEntityID", IntegerType(), True),
    StructField("TotalPurchaseYTD", DecimalType(19, 4), True),
    StructField("DateFirstPurchase", TimestampType(), True),
    StructField("BirthDate", TimestampType(), True),
    StructField("MaritalStatus", StringType(), True),
    StructField("YearlyIncome", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("TotalChildren", IntegerType(), True),
    StructField("NumberChildrenAtHome", IntegerType(), True),
    StructField("Education", StringType(), True),
    StructField("Occupation", StringType(), True),
    StructField("HomeOwnerFlag", BooleanType(), True),
    StructField("NumberCarsOwned", IntegerType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
