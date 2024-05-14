INSERT INTO [Metadata].[lh].[SilverGoldDependencies]
VALUES 
    ('common_Date', 'dim_Calendar'), 
    ('common_CurrencyExchange', 'fact_Sales'),
    ('common_SalesOrderHeader', 'fact_Sales'),
    ('common_SalesOrderLine', 'fact_Sales'),
    ('common_Customer', 'dim_Customer'),
    ('common_Store', 'dim_Store'),
    ('common_Product', 'dim_Product')