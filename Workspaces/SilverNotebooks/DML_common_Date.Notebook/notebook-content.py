# PARAMETERS CELL ********************

LoadMode = "Type of loading the data; 1 - incremental, 0 - full"
Watermark = "Timestamp of the recent load to Silver-Operational"

# CELL ********************

%run nb_IncrementalLoader

# CELL ********************

df = spark.sql("""
SELECT
    Date,
    DateKey,
    Year,
    YearQuarter,
    YearQuarterNumber,
    Quarter,
    YearMonth,
    YearMonthShort,
    YearMonthNumber,
    Month,
    MonthShort,
    MonthNumber,
    DayOfWeek,
    DayOfWeekShort,
    DayOfWeekNumber,
    WorkingDay,
    WorkingDayNumber
FROM
    CS_main_Date
""")


# CELL ********************

il = IncrementalLoader(LoadMode, source_watermark=Watermark, df=df)
il.analyze()

# CELL ********************

returnmsg = json.dumps({'RowsInserted': il.rows_inserted, 'RowsUpdated': il.rows_updated, 'Watermark': il.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
