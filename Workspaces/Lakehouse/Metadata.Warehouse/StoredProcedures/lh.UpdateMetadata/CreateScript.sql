CREATE PROCEDURE [lh].[UpdateMetadata]
(
	@Stage NVARCHAR(50),
	@SourceSystem NVARCHAR(50),
	@SourceSchema NVARCHAR(50),
	@SourceTable NVARCHAR(50),
	@RowCount INT, 
	@Watermark DATETIME
)
AS
BEGIN  
    SET NOCOUNT ON
	IF @Stage = 'Bronze-Landing'
	BEGIN
		UPDATE [Metadata].[lh].[SourceEntity] 
		SET BronzeLandingTimestamp = @Watermark, 
			RowsIngestedBronzeLanding = @RowCount
		WHERE SourceSystem = @SourceSystem
			AND SourceSchema= @SourceSchema
			AND SourceTable = @SourceTable
	END

	IF @Stage = 'Bronze-Raw'
	BEGIN
		UPDATE [Metadata].[lh].[SourceEntity] 
		SET BronzeRawTimestamp = @Watermark, 
			RowsIngestedBronzeRaw = @RowCount
		WHERE SourceSystem = @SourceSystem
			AND SourceSchema= @SourceSchema
			AND SourceTable = @SourceTable
	END

	IF @Stage = 'Silver-Operational'
	BEGIN
		UPDATE [Metadata].[lh].[SourceEntity] 
		SET SilverOperationalTimestamp = @Watermark, 
			RowsIngestedSilverOperational = @RowCount
		WHERE SourceSystem = @SourceSystem
			AND SourceSchema= @SourceSchema
			AND SourceTable = @SourceTable
	END

	IF @Stage = 'Silver-Conformed'
	BEGIN
		UPDATE [Metadata].[lh].[SilverEntity] 
		SET SilverConformedTimestamp = @Watermark, 
			RowsIngestedSilverConformed = @RowCount
		WHERE SilverSystem = @SourceSystem
			AND SilverSchema= @SourceSchema
			AND SilverTable = @SourceTable
	END

	IF @Stage = 'Gold'
	BEGIN
		UPDATE [Metadata].[lh].[GoldEntity] 
		SET GoldTimestamp = @Watermark, 
			RowsIngestedGold = @RowCount
		WHERE GoldSystem = @SourceSystem
			AND GoldSchema= @SourceSchema
			AND GoldTable = @SourceTable
	END
		
END
