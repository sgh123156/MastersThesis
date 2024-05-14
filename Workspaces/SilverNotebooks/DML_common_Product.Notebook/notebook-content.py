# PARAMETERS CELL ********************

LoadMode = "Type of loading the data; 1 - incremental, 0 - full"
Watermark = "Timestamp of the recent load to Silver-Operational"

# CELL ********************

%run nb_IncrementalLoader

# CELL ********************

df = spark.sql("""
    SELECT
    p.Hashkey,
    p.ProductID AS ProductKey,
    p.ProductNumber AS ProductCode,
    p.Name AS ProductName,
    'Adventure Works' AS Manufacturer,
    'Adventure Works' AS Brand,
    IFNULL(p.Color, 'Unknown') AS Color,
    um.Name AS WeightUnitMeasure,
    p.Weight,
    p.StandardCost AS UnitCost,
    p.ListPrice AS UnitPrice,
    p.ProductSubcategoryID AS SubcategoryCode,
    ps.Name AS Subcategory,
    pc.ProductCategoryID AS CategoryCode,
    pc.Name AS Category,
    p.ProductLine,
    p.Source AS Source,
    CURRENT_TIMESTAMP() AS SilverConformedTimestamp
FROM AW_Production_Product p
    LEFT JOIN AW_Production_UnitMeasure um 
        ON um.UnitMeasureCode = p.WeightUnitMeasureCode
    LEFT JOIN AW_Production_ProductSubcategory ps 
        ON ps.ProductSubcategoryID = p.ProductSubcategoryID
    LEFT JOIN AW_Production_ProductCategory pc 
        ON pc.ProductCategoryID = ps.ProductCategoryID

UNION ALL 

SELECT
    HashKey,
    ProductKey,
    ProductCode,
    ProductName,
    Manufacturer,
    Brand,
    Color,
    CASE WeightUnitMeasure
        WHEN 'ounces' THEN 'Ounce'
        WHEN 'grams' THEN 'Gram'
        WHEN 'pounds' THEN 'US Pound'
        ELSE WeightUnitMeasure
        END AS WeightUnitMeasure,
    Weight,
    UnitCost,
    UnitPrice,
    SubcategoryCode,
    Subcategory,
    CategoryCode,
    Category,
    'N/A' AS ProductLine,
    Source,
    CURRENT_TIMESTAMP() AS SilverConformedTimestamp 
FROM
    CS_main_Product
""")

# CELL ********************

il = IncrementalLoader(LoadMode, source_watermark=Watermark, df=df)
il.analyze()

# CELL ********************

returnmsg = json.dumps({'RowsInserted': il.rows_inserted, 'RowsUpdated': il.rows_updated, 'Watermark': il.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
