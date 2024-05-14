# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS gold_dim_Product (
    HBK_Product STRING,
    HashKey STRING, 
    ProductCode STRING,
    ProductName STRING,
    Manufacturer STRING,
    Brand STRING,
    Color STRING,
    WeightUnitMeasure STRING,
    Weight DOUBLE,
    UnitCost DECIMAL(19,4),
    UnitPrice DECIMAL(19,4),
    Subcategory STRING,
    Category STRING,
    ProductFamily STRING,
    GoldTimestamp TIMESTAMP, 
    ActiveFrom TIMESTAMP,
    IsActive BOOLEAN
    )
USING DELTA
'''
)
