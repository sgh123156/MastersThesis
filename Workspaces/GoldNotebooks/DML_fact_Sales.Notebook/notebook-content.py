# PARAMETERS CELL ********************

LoadMode = "Type of loading the data; 1 - incremental (SCD1), 0 - full, 2 - incremental with SCD2"
SCD2Columns = "Names of columns in LoadMode2 to be considered for SCD2"
Watermark = "Timestamp of the recent load to Silver-Confomed"

# CELL ********************

%run nb_ModelLoader

# CELL ********************

df = spark.sql("""

SELECT 
    sol.HashKey, 
    soh.OrderKey,
    sol.LineNumber,
    OrderDate, 
    DueDate,
    soh.CurrencyCode, 
    c.HBK_Customer,
    s.HBK_Store, 
    p.HBK_Product,
    soh.SilverConformedTimestamp,
    CURRENT_TIMESTAMP() AS GoldTimestamp,
    True As IsActive
FROM Silver.silver_common_SalesOrderLine sol 
    INNER JOIN Silver.silver_common_SalesOrderHeader soh 
        ON sol.OrderKey = soh.OrderKey AND sol.Source = soh.Source
    LEFT JOIN Gold.gold_dim_customer c
        ON c.HashKey = soh.HashKey_Customer
            --AND CURRENT_TIMESTAMP() BETWEEN c.ActiveFrom AND c.ActiveTo
            AND c.IsActive = True
    LEFT JOIN Gold.gold_dim_product p
        ON p.HashKey = sol.Hashkey_Product
    LEFT JOIN Gold.gold_dim_Store s
        ON s.HashKey = soh.HashKey_Store
""")


# CELL ********************

ml = ModelLoader(df,LoadMode, SCD2Columns, Watermark)
ml.analyze()

# CELL ********************

returnmsg = json.dumps({'RowsInserted': ml.rows_inserted, 'RowsUpdated': ml.rows_updated, 'Watermark': ml.target_watermark.strftime('%Y-%m-%dT%H:%M:%S')})
returnmsg
mssparkutils.notebook.exit(returnmsg)
