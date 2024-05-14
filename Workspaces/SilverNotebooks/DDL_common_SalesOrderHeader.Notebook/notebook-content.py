# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS silver_common_SalesOrderHeader (
    HashKey STRING,
    OrderKey STRING,
    CustomerKey INT,
    StoreKey INT,
    OrderDate DATE,
    DueDate DATE,
    CurrencyCode STRING,
    OnlineOrderFlag BOOLEAN,
    SalesOrderNumber STRING,
    Source STRING,
    SilverOperationalTimestamp TIMESTAMP,
    SilverConformedTimestamp TIMESTAMP,
    HashKey_Customer STRING,
    HashKey_Store STRING
)
USING DELTA
'''
)
