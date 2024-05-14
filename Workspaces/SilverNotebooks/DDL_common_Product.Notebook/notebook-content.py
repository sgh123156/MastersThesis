# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS silver_common_Product (
    HashKey STRING,
    ProductKey INT,
    ProductCode STRING,
    ProductName STRING,
    Manufacturer STRING,
    Brand STRING,
    Color STRING,
    WeightUnitMeasure STRING,
    Weight DOUBLE,
    UnitCost DECIMAL(19,4),
    UnitPrice DECIMAL(19,4),
    SubcategoryCode STRING,
    Subcategory STRING,
    CategoryCode STRING,
    Category STRING,
    ProductLine STRING,
    Source STRING,
    SilverConformedTimestamp TIMESTAMP 
    )
USING DELTA
''')
