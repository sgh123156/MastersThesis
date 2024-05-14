# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS silver_common_Customer (
    HashKey STRING,
    CustomerKey INT,
    Gender STRING,
    Title STRING,
    FirstName STRING,
    MiddleInitial STRING,
    LastName STRING,
    StreetAddress STRING,
    City STRING,
    State STRING,
    PostalCode STRING,
    CountryCode STRING,
    Country STRING,
    BirthDate DATE,
    Occupation STRING,
    Source STRING,
    SilverConformedTimestamp TIMESTAMP NOT NULL
)
USING DELTA
'''
)
