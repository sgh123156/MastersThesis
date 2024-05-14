INSERT INTO [Metadata].[lh].[GoldEntity]
VALUES 
    (1, 'gold', 'fact', 'Sales', 1, NULL, '2000-01-01', NULL),
    (1, 'gold', 'dim', 'Calendar', 1, NULL, '2000-01-01', NULL),
    (1, 'gold', 'dim', 'Product', 1, NULL, '2000-01-01', NULL),
    (1, 'gold', 'dim','Customer', 2, 'Country, State, City', '2000-01-01', NULL),
    (1, 'gold', 'dim','Store', 1, NULL, '2000-01-01', NULL)