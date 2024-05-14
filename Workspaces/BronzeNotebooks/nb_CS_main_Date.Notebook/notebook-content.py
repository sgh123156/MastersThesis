# CELL ********************

from pyspark.sql.types import *
import json

# CELL ********************

schema = StructType([
    StructField("Date", DateType(), True),
    StructField("DateKey", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("YearQuarter", StringType(), True),
    StructField("YearQuarterNumber", IntegerType(), True),
    StructField("Quarter", StringType(), True),
    StructField("YearMonth", StringType(), True),
    StructField("YearMonthShort", StringType(), True),
    StructField("YearMonthNumber", IntegerType(), True),
    StructField("Month", StringType(), True),
    StructField("MonthShort", StringType(), True),
    StructField("MonthNumber", IntegerType(), True),
    StructField("DayOfWeek", StringType(), True),
    StructField("DayOfWeekShort", StringType(), True),
    StructField("DayOfWeekNumber", IntegerType(), True),
    StructField("WorkingDay", BooleanType(), True),
    StructField("WorkingDayNumber", IntegerType(), True),
    StructField("BronzeLandingTimestamp", StringType(), True),
    StructField("Source", StringType(), True)
])

# CELL ********************

mssparkutils.notebook.exit(schema.json())
