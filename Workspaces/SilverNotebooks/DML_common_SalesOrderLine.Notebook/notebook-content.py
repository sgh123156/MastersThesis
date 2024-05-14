# PARAMETERS CELL ********************

LoadMode = "Type of loading the data; 1 - incremental, 0 - full"
Watermark = "Timestamp of the recent load to Silver-Operational"

# CELL ********************

%run nb_IncrementalLoader

# CELL ********************

df = spark.sql("""

SELECT sub.*, 
        sha2(CONCAT(Source,'|',  ProductKey), 256) AS HashKey_Product
FROM (
    SELECT 
        sod.HashKey,
        sod.SalesOrderID AS OrderKey,
        sod.SalesOrderDetailID AS LineNumber,
        sod.ProductID AS ProductKey,
        sod.OrderQty AS Quantity,
        sod.UnitPrice,
        (sod.UnitPrice - sod.UnitPriceDiscount) AS NetPrice,
        p.StandardCost AS UnitCost,
        sod.Source,
        sod.SilverOperationalTimestamp,
        CURRENT_TIMESTAMP() AS SilverConformedTimestamp
    FROM 
        AW_Sales_SalesOrderDetail sod
        LEFT JOIN AW_Production_Product p ON p.ProductID = sod.ProductID AND p.Source = sod.Source

    UNION ALL

    SELECT 
        HashKey,
        OrderKey,
        LineNumber,
        ProductKey,
        Quantity,
        UnitPrice,
        NetPrice,
        UnitCost,
        Source,
        SilverOperationalTimestamp,
        CURRENT_TIMESTAMP() AS SilverConformedTimestamp
    FROM 
        CS_main_OrderRows
) sub
""")

# CELL ********************

il = IncrementalLoader(LoadMode, source_watermark=Watermark, df=df)
il.analyze()

# CELL ********************

returnmsg = json.dumps({'RowsInserted': il.rows_inserted, 'RowsUpdated': il.rows_updated, 'Watermark': il.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
