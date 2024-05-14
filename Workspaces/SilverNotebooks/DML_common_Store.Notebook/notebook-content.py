# PARAMETERS CELL ********************

LoadMode = "Type of loading the data; 1 - incremental, 0 - full"
Watermark = "Timestamp of the recent load to Silver-Operational"

# CELL ********************

%run nb_IncrementalLoader

# CELL ********************

df = spark.sql("""
SELECT 
    s.HashKey,
    s.BusinessEntityID AS StoreKey,
    s.Name,
    cr.Name AS Country,
    sp.Name AS State,
    ROUND(SquareFeet * 0.092903, 0) AS SquareMeters,
    YearOpened,
    NumberEmployees,
    s.Source, 
    CURRENT_TIMESTAMP() AS SilverConformedTimestamp
FROM  AW_Sales_vStoreWithDemographics s
    LEFT JOIN AW_Person_BusinessEntityAddress a ON a.BusinessEntityID = s.BusinessEntityID
    LEFT JOIN AW_Person_AddressType at ON at.AddressTypeID = a.AddressTypeID
    LEFT JOIN AW_Person_Address ad ON ad.AddressID = a.AddressID
    LEFT JOIN AW_Person_StateProvince sp ON sp.StateProvinceID = ad.StateProvinceID
    LEFT JOIN AW_Person_CountryRegion cr ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE 
    at.Name = 'Main Office'

UNION ALL

SELECT 
    HashKey,
    StoreKey,
    Name,
    Country,
    State,
    SquareMeters,
    YEAR(OpenDate) AS YearOpened,
    50 AS NumberEmployees,
    Source, 
    CURRENT_TIMESTAMP() AS SilverConformedTimestamp
FROM CS_main_Store
""")

# CELL ********************

il = IncrementalLoader(LoadMode, source_watermark=Watermark, df=df)
il.analyze()

# CELL ********************

returnmsg = json.dumps({'RowsInserted': il.rows_inserted, 'RowsUpdated': il.rows_updated, 'Watermark': il.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
