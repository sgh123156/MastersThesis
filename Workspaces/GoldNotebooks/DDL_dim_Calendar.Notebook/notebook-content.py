# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS Gold.gold_dim_Calendar (
    HBK_Calendar STRING,
    HashKey DATE,
    Date DATE,
    Year INT,
    YearQuarter STRING,
    Quarter STRING,
    YearMonth STRING,
    YearMonthShort STRING,
    Month STRING,
    MonthShort STRING,
    MonthNumber INT,
    DayOfWeek STRING,
    WorkingDay BOOLEAN,
    GoldTimestamp TIMESTAMP NOT NULL,
    ActiveFrom TIMESTAMP,
    IsActive BOOLEAN
)
USING DELTA
'''
)