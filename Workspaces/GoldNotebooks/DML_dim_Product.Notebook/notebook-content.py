# PARAMETERS CELL ********************

LoadMode = "Type of loading the data; 1 - incremental (SCD1), 0 - full, 2 - incremental with SCD2"
SCD2Columns = "Names of columns in LoadMode2 to be considered for SCD2"
Watermark = "Timestamp of the recent load to Silver-Confomed"

# CELL ********************

%run nb_ModelLoader

# CELL ********************

df = spark.sql("""

SELECT 
    HashKey,
    ProductCode, 
    ProductName, 
    Manufacturer, 
    Brand, 
    Color, 
    WeightUnitMeasure, 
    Weight, 
    UnitCost, 
    UnitPrice, 
    Subcategory, 
    Category,
    CASE WHEN Category LIKE '%amera%' THEN 'Family A' ELSE 'Family B' END AS ProductFamily,
    SilverConformedTimestamp,
    CURRENT_TIMESTAMP() AS GoldTimestamp,
    True As IsActive
FROM Silver.silver_common_Product
""")

# CELL ********************

ml = ModelLoader(df,LoadMode, SCD2Columns, Watermark)
ml.analyze()

# CELL ********************

returnmsg = json.dumps({'RowsInserted': ml.rows_inserted, 'RowsUpdated': ml.rows_updated, 'Watermark': ml.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
