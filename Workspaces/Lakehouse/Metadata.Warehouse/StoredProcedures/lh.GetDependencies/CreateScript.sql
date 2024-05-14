CREATE PROCEDURE [lh].[GetDependencies] 
(
	@Table VARCHAR(50), 
	@Schema VARCHAR(50), 
	@FromTo VARCHAR(50)
)
AS
BEGIN
	IF @FromTo = 'OperationalConformed' 
	BEGIN
		SELECT se.SilverTable, MAX(CAST(IsCurrentlyProcessed AS INT)) NotToRun
		FROM [lh].[SilverEntity] se
			INNER JOIN [lh].[BronzeSilverDependencies] bsd
				ON CONCAT(se.SilverSchema,'_', se.SilverTable) = bsd.SilverTable
			INNER JOIN [lh].[RunningProcesses] rp
				ON CONCAT(rp.SourceSystem, '_', rp.SourceSchema, '_', rp.SourceTable) = bsd.BronzeTable
		WHERE se.SilverSchema = @Schema AND se.SilverTable = @Table
		GROUP BY se.SilverTable
	END

	IF @FromTo = 'ConformedGold' 
	BEGIN
		SELECT GoldSchema, GoldTable, MAX(CAST(IsCurrentlyProcessed AS INT)) NotToRun 
		FROM (
			SELECT ge.GoldSchema,ge.GoldTable, sgd.SilverTable, rp1.IsCurrentlyProcessed
			FROM [lh].[GoldEntity] ge
				INNER JOIN [lh].[SilverGoldDependencies] sgd
					ON CONCAT(ge.GoldSchema,'_', ge.GoldTable) = sgd.GoldTable
				INNER JOIN  [lh].[RunningProcesses] rp1
					ON CONCAT(rp1.SourceSchema, '_', rp1.SourceTable) = sgd.SilverTable
			UNION ALL
			SELECT ge.GoldSchema,ge.GoldTable, dfd.DimTable, rp2.IsCurrentlyProcessed
			FROM [lh].[GoldEntity] ge
				INNER JOIN [lh].[DimFactDependencies] dfd
					ON CONCAT(ge.GoldSchema,'_', ge.GoldTable) = dfd.FactTable
				INNER JOIN  [lh].[RunningProcesses] rp2
					ON CONCAT(rp2.SourceSchema, '_', rp2.SourceTable) = dfd.DimTable
		) sub
		WHERE GoldSchema = @Schema AND GoldTable = @Table
		GROUP BY GoldSchema, GoldTable
	END

END