use dmk_stage_db
go

drop table if exists raw.city
go
CREATE TABLE [raw].[city](
	[city_id] [nvarchar](128) NOT NULL,
	[name] [nvarchar](128) NULL,
	[locale_id] [nvarchar](128) NOT NULL,
	[date_created] [smalldatetime] NOT NULL,
	[master_record_id] [nvarchar](128) NULL
) ON [PRIMARY]
GO

drop table if exists dbo.city
go
CREATE TABLE dbo.[city](
	[city_id] [int]  NOT NULL,
	[name] [nvarchar](128) NULL,
	[locale_id] [int] NOT NULL,
	[master_record_id] [int] NULL,
	job_inst_id int not null,
	[date_created] [smalldatetime] NOT NULL default getdate()

) ON [PRIMARY]
GO