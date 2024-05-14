CREATE TABLE [lh].[SilverEntity](
	[IsEnabled] [bit] NULL,
	[SilverSystem] [varchar](50) NULL,
	[SilverSchema] [varchar](50) NULL,
	[SilverTable] [varchar](150) NULL,
	[SilverConformedTimestamp] [datetime2](2) NULL,
	[RowsIngestedSilverConformed] [int] NULL
) ON [PRIMARY]