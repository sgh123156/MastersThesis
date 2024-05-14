INSERT INTO [Metadata].[lh].[DimFactDependencies]
VALUES 
    ('dim_Customer', 'fact_Sales'),
    ('dim_Store', 'fact_Sales'),
    ('dim_Product', 'fact_Sales')