USE [dmk_stage_db]
GO

/****** Object:  Table [raw].[kafka_topic]    Script Date: 5/13/2025 11:50:21 AM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[raw].[kafka_topic]') AND type in (N'U'))
DROP TABLE [raw].[kafka_topic]


CREATE TABLE [raw].[kafka_topic](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[kafka_topic] varchar(max) null,
	[name] [varchar](max) NULL,
	[address] [varchar](max) NULL,
	[transaction_id] [varchar](max) NULL,
	[amount] [bigint] NULL,
	[group_id] [varchar](max) NULL,
	[date_created] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


