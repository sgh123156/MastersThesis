# PARAMETERS CELL ********************

LoadMode = "Type of loading the data; 1 - incremental, 0 - full"
Watermark = "Timestamp of the recent load to Silver-Operational"

# CELL ********************

%run nb_IncrementalLoader

# CELL ********************

df = spark.sql("""

SELECT sub.*, 
        sha2(CONCAT(Source,'|',  CustomerKey), 256) AS HashKey_Customer,
        sha2(CONCAT(Source,'|',  StoreKey), 256) AS HashKey_Store
FROM (
    SELECT 
        soh.HashKey,
        soh.SalesOrderID AS OrderKey,
        soh.CustomerID AS CustomerKey,
        c.StoreID AS StoreKey,
        CAST(soh.OrderDate AS DATE) AS OrderDate,
        CAST(soh.DueDate AS DATE) AS DueDate,
        crc.CurrencyCode,
        soh.OnlineOrderFlag,
        soh.SalesOrderNumber, 
        soh.Source, 
        soh.SilverOperationalTimestamp,
        CURRENT_TIMESTAMP() AS SilverConformedTimestamp
    FROM 
        AW_Sales_SalesOrderHeader soh
        LEFT JOIN AW_Sales_Customer c ON c.CustomerID = soh.CustomerID
        LEFT JOIN AW_Sales_SalesTerritory st ON soh.TerritoryID = st.TerritoryID
        LEFT JOIN (
            SELECT 
                CountryRegionCode,
                CurrencyCode,
                ModifiedDate,
                ROW_NUMBER() OVER(PARTITION BY CountryRegionCode ORDER BY ModifiedDate DESC) AS rownr
            FROM AW_Sales_CountryRegionCurrency
        ) crc
            ON crc.CountryRegionCode = st.CountryRegionCode AND crc.rownr = 1

    UNION ALL

    SELECT
        HashKey,
        OrderKey,
        CustomerKey,
        StoreKey,
        OrderDate,
        date_add(OrderDate, 7) AS DueDate,
        CurrencyCode,
        False AS OnlineOrderFlag,
        'Not applicable' AS SalesOrderNumber,
        Source, 
        SilverOperationalTimestamp,
        CURRENT_TIMESTAMP() AS SilverConformedTimestamp
    FROM 
        CS_main_Orders
) sub
""")

# CELL ********************

il = IncrementalLoader(LoadMode, source_watermark=Watermark, df=df)
il.analyze()

# CELL ********************

returnmsg = json.dumps({'RowsInserted': il.rows_inserted, 'RowsUpdated': il.rows_updated, 'Watermark': il.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
