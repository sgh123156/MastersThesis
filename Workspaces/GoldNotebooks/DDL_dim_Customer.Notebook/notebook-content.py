# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS Gold.gold_dim_Customer (

    HBK_Customer STRING,
    HashKey STRING,
    CustomerCode INT,
    Gender STRING,
    Title STRING,
    FirstName STRING,
    MiddleInitial STRING,
    LastName STRING,
    StreetAddress STRING,
    City STRING,
    State STRING,
    PostalCode STRING,
    Country STRING,
    Occupation STRING,
    GoldTimestamp TIMESTAMP NOT NULL,
    ActiveFrom TIMESTAMP,
    IsActive BOOLEAN,
    ActiveTo TIMESTAMP
)
USING DELTA
'''
)
