# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS silver_common_Date (
    Date DATE,
    DateKey INT,
    Year INT,
    YearQuarter STRING,
    YearQuarterNumber INT,
    Quarter STRING,
    YearMonth STRING,
    YearMonthShort STRING,
    YearMonthNumber INT,
    Month STRING,
    MonthShort STRING,
    MonthNumber INT,
    DayOfWeek STRING,
    DayOfWeekShort STRING,
    DayOfWeekNumber INT,
    WorkingDay BOOLEAN,
    WorkingDayNumber INT
)
USING DELTA
'''
)
