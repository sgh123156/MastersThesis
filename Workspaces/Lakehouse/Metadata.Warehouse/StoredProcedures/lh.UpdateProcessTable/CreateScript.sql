CREATE PROCEDURE [lh].[UpdateProcessTable]
(
    @System NVARCHAR(50),
	@Schema NVARCHAR(50),
    @Table NVARCHAR(50),
    @WhenExecuted NVARCHAR(30) = NULL,
    @PipelineID NVARCHAR(50) = NULL

)
AS
BEGIN
    SET NOCOUNT ON;

	IF @WhenExecuted LIKE '%in'
	BEGIN
		DELETE FROM [lh].[RunningProcesses] WHERE SourceSystem = @System;

		INSERT INTO [lh].[RunningProcesses]
        SELECT   'Bronze'
				,SourceSystem
				,SourceSchema
				,SourceTable
				,1
				,@PipelineID
		FROM [lh].SourceEntity se
		WHERE  se.IsEnabled = 1
			AND se.SourceSystem = @System

		UNION ALL

        SELECT   'Silver'
				,SilverSystem
				,SilverSchema
				,SilverTable
				,1
				,@PipelineID
		FROM [lh].SilverEntity sv
		WHERE  sv.IsEnabled = 1
			AND sv.SilverSystem = @System

		UNION ALL

        SELECT   'Gold'
				,GoldSystem
				,GoldSchema
				,GoldTable
				,1
				,@PipelineID
		FROM [lh].GoldEntity ge
		WHERE  ge.IsEnabled = 1
			AND ge.GoldSystem = @System

	END

	IF @WhenExecuted LIKE '%out'
	BEGIN
		UPDATE rp
		SET rp.IsCurrentlyProcessed = 0,
			rp.ProcessingPipeline = 'Not processed'
		FROM [lh].[RunningProcesses]  rp
		WHERE SourceSchema = @Schema
			AND SourceSystem =  @System
			AND SourceTable = @Table
	END

END;
