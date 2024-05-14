# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS silver_common_Store (
    HashKey STRING,
    StoreKey INT,
    Name STRING,
    Country STRING,
    State STRING,
    SquareMeters DECIMAL(12,0),
    YearOpened INT,
    NumberEmployees INT,
    Source STRING,
    SilverConformedTimestamp TIMESTAMP 
)
USING DELTA
'''
)
