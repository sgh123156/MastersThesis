CREATE PROCEDURE [lh].[GetMetadata] 
(
    @SourceSystem NVARCHAR(50),
	@Mode NVARCHAR(10) = 'init'
)
AS
BEGIN
    SET NOCOUNT ON

    DECLARE @columns AS NVARCHAR(MAX), @sql AS NVARCHAR(MAX);

	SELECT @columns = STRING_AGG(QUOTENAME(Resource), ',')
	FROM [Metadata].[lh].[TechnicalResources];

    SET @sql = N'
    SELECT se.*, p.*, 
			CASE se.LoadMode 
               WHEN 0 THEN NULL 
               ELSE CONCAT('' WHERE '', se.SourceDateColumn, '' >= '''''' , CONVERT(varchar, DATEADD(day, -3, ISNULL(se.BronzeLandingTimestamp, ''1901-01-01'')), 121) , '''''''' )
           END AS LandingTSPredicate 
		   ,CASE se.LoadMode WHEN 0 THEN '''' ELSE FORMAT(GETDATE(), ''yyyy/MM/dd/HHmm'') END AS IncrementalPath
		   ,CASE WHEN se.SoftDeletes = 1 AND DATEPART(WEEKDAY, GETDATE()) IN (1, 3, 5) THEN SourceSystem ELSE '''' END AS ValidForSoftDeletes
    FROM 
    (
        SELECT' + @columns + ' 
        FROM [Metadata].[lh].[TechnicalResources]
        PIVOT (
            MAX(ResourceValue)
            FOR Resource IN (' + @columns + ')
        ) AS PivotTable
    ) p
    CROSS JOIN [lh].[SourceEntity] se
    WHERE se.SourceSystem = @SourceSystem AND se.IsEnabled = 1'
		;

    EXEC sp_executesql @sql, N'@SourceSystem NVARCHAR(50)', @SourceSystem;
END