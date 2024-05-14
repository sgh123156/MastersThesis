# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS silver_common_SalesOrderLine (
    HashKey STRING,
    OrderKey BIGINT,
    LineNumber INT,
    ProductKey INT,
    Quantity INT,
    UnitPrice DECIMAL(19,4),
    NetPrice DECIMAL(20,4),
    UnitCost DECIMAL(19,4),
    Source STRING,
    SilverOperationalTimestamp TIMESTAMP,
    SilverConformedTimestamp TIMESTAMP NOT NULL,
    HashKey_Product STRING
)
USING DELTA
'''
)
