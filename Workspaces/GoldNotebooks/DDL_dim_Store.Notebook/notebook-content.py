# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS Gold.gold_dim_Store (
    HBK_Store STRING,
    HashKey STRING,
    Name STRING,
    Country STRING,
    State STRING,
    SquareMeters DECIMAL(12,0),
    StoreScale STRING ,
    GoldTimestamp TIMESTAMP NOT NULL,
    ActiveFrom TIMESTAMP,
    IsActive BOOLEAN
)
USING DELTA
'''
)
