# PARAMETERS CELL ********************

LoadMode = "Type of loading the data; 1 - incremental, 0 - full"
Watermark = "Timestamp of the recent load to Silver-Operational"

# CELL ********************

%run nb_IncrementalLoader

# CELL ********************

df = spark.sql("""
SELECT 
    c.HashKey,
    c.CustomerID AS CustomerKey,
    CASE pd.Gender 
        WHEN 'F' THEN 'Female' 
        WHEN 'M' THEN 'Male' 
        ELSE 'Undisclosed' 
        END AS Gender,
    p.Title,
    p.FirstName,
    LEFT(p.MiddleName, 1) AS MiddleInitial,
    p.LastName, 
    a.AddressLine1 AS StreetAddress,
    a.City,
    sp.Name AS State,
    a.PostalCode,
    cr.CountryRegionCode AS CountryCode,
    cr.Name AS Country,
    CAST(pd.BirthDate AS DATE) AS BirthDate,
    pd.Occupation,
    c.Source,
    CURRENT_TIMESTAMP() AS SilverConformedTimestamp
FROM AW_Sales_Customer c
    INNER JOIN AW_Person_Person p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN AW_Sales_vPersonDemographics pd ON pd.BusinessEntityID = c.PersonID
    LEFT JOIN AW_Person_BusinessEntityAddress b ON b.BusinessEntityID = p.BusinessEntityID AND b.AddressTypeID = 2
    LEFT JOIN AW_Person_Address a ON a.AddressID = b.AddressID
    LEFT JOIN AW_Person_StateProvince sp ON sp.StateProvinceID = a.StateProvinceID
    LEFT JOIN AW_Person_CountryRegion cr ON cr.CountryRegionCode = sp.CountryRegionCode

UNION ALL

SELECT 
    HashKey,
    CustomerKey,
    Gender,
    Title,
    GivenName AS FirstName,
    MiddleInitial,
    Surname AS LastName,
    StreetAddress,
    City,
    StateFull AS State,
    ZipCode AS PostalCode,
    Country AS CountryCode,
    CountryFull AS Country,
    CAST(Birthday AS DATE) AS BirthDate,
    Occupation,
    Source,
    CURRENT_TIMESTAMP() AS SilverConformedTimestamp
FROM 
    CS_main_Customer
""")

# CELL ********************

il = IncrementalLoader(LoadMode, source_watermark=Watermark, df=df)
il.analyze()

# CELL ********************

returnmsg = json.dumps({'RowsInserted': il.rows_inserted, 'RowsUpdated': il.rows_updated, 'Watermark': il.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
