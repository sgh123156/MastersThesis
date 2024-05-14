CREATE PROCEDURE [dbo].[ViewCreator]
	@SourceSystem NVARCHAR(50),
    @SourceSchema NVARCHAR(128),  --becomes the schema name
    @SourceTable NVARCHAR(128)

AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL_Drop NVARCHAR(MAX);
	DECLARE @SQL_Create NVARCHAR(MAX);
    DECLARE @Columns NVARCHAR(MAX);

	SET @SQL_Drop = 'DROP VIEW IF EXISTS ' + QUOTENAME(@SourceSystem) + '.' + CONCAT('[', @SourceSchema, '_', @SourceTable, ']') + ';';
	EXEC sp_executesql @SQL_Drop;

	PRINT 'View has been dropped.'

	SET @Columns =  (
		SELECT STRING_AGG(CONCAT('[', c.name, ']'), ', ')    
		FROM sys.columns AS c
			INNER JOIN sys.tables AS t ON c.object_id = t.object_id
			INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
		WHERE t.name = CONCAT(@SourceSystem, '_', @SourceSchema, '_',  @SourceTable)                         
			AND s.name = 'dbo'
			AND c.name NOT IN ('Source', 'HashKey', 'IsDeleted') AND c.name NOT LIKE '%Timestamp'
		);

    SET @SQL_Create = 'CREATE VIEW ' + QUOTENAME(@SourceSystem) + '.' + CONCAT('[', @SourceSchema, '_', @SourceTable, ']')
             + ' AS SELECT ' + @Columns + ' FROM ' +  'dbo.' + CONCAT(@SourceSystem, '_', @SourceSchema, '_',  @SourceTable) + ';';

    EXEC sp_executesql @SQL_Create;

	PRINT 'View has been created.'

END
