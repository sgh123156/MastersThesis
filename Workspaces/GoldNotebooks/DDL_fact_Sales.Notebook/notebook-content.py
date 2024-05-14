# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS gold_fact_Sales (
    HBK_Sales STRING,
    HashKey STRING,
    OrderKey STRING,
    LineNumber INT,
    OrderDate DATE,
    DueDate DATE,
    CurrencyCode STRING,
    HBK_Customer STRING,
    HBK_Store STRING,
    HBK_Product STRING,
    GoldTimestamp TIMESTAMP,
    IsActive BOOLEAN,
    ActiveFrom TIMESTAMP
)
USING DELTA
'''
)
