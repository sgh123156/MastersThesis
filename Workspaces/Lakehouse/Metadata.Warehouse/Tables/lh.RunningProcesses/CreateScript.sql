CREATE TABLE [lh].[RunningProcesses](
	[Layer] [varchar](20) NULL,
	[SourceSystem] [varchar](50) NULL,
	[SourceSchema] [varchar](50) NULL,
	[SourceTable] [varchar](50) NULL,
	[IsCurrentlyProcessed] [bit] NULL,
	[ProcessingPipeline] [varchar](100) NULL
) ON [PRIMARY]
