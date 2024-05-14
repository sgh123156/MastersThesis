# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("RevisionNumber", ShortType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("ShipDate", TimestampType(), True),
    StructField("Status", ShortType(), True),
    StructField("OnlineOrderFlag", BooleanType(), True),
    StructField("SalesOrderNumber", StringType(), True),
    StructField("PurchaseOrderNumber", StringType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("SalesPersonID", IntegerType(), True),
    StructField("TerritoryID", IntegerType(), True),
    StructField("BillToAddressID", IntegerType(), True),
    StructField("ShipToAddressID", IntegerType(), True),
    StructField("ShipMethodID", IntegerType(), True),
    StructField("CreditCardID", IntegerType(), True),
    StructField("CreditCardApprovalCode", StringType(), True),
    StructField("CurrencyRateID", IntegerType(), True),
    StructField("SubTotal", DecimalType(19, 4), True),
    StructField("TaxAmt", DecimalType(19, 4), True),
    StructField("Freight", DecimalType(19, 4), True),
    StructField("TotalDue", DecimalType(19, 4), True),
    StructField("Comment", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
