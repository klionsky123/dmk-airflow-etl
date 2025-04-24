use dmk_stage_db
go

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'metadata'
)
BEGIN
    EXEC('CREATE SCHEMA metadata')
END
go

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'extract'
)
BEGIN
    EXEC('CREATE SCHEMA extract')
END
go

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'transform'
)
BEGIN
    EXEC('CREATE SCHEMA transform')
END
go
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'raw'
)
BEGIN
    EXEC('CREATE SCHEMA raw')
END
--------------------------
go
--drop table if exists metadata.job_scheduler
drop table if exists metadata.job_inst_task
drop table if exists metadata.job_inst
drop table if exists metadata.job_task
drop table if exists metadata.job
drop table if exists metadata.tbl
drop table if exists metadata.conn_str
drop table if exists metadata.conn_str_type
drop table if exists metadata.date_source

create table metadata.data_source(
 data_source_id int identity not null
,data_source_name varchar(256) not null
,descr varchar(max) null
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_data_source_PK PRIMARY KEY (data_source_id)
,CONSTRAINT metadata_data_source_UQ UNIQUE (data_source_name)
)

create table metadata.conn_str_type(
 conn_str_type_id int identity not null
,conn_str_type_name varchar(256) not null
,descr varchar(max) null
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_conn_str_type_PK PRIMARY KEY (conn_str_type_id)
,CONSTRAINT metadata_conn_str_type_UQ UNIQUE (conn_str_type_name)
)

create table metadata.conn_str(
	 conn_str_id int identity not null
	,conn_str_name varchar(256) not null
	,parent_conn_str_id int null		/* for api key dependency */
	,conn_str_type_id int not null CONSTRAINT metadata_conn_str_FK1 FOREIGN KEY REFERENCES metadata.conn_str_type(conn_str_type_id)
	,data_source_id int not null CONSTRAINT metadata_conn_str_FK2 FOREIGN KEY REFERENCES metadata.data_source(data_source_id)
	,username varchar(128) null
	,pass varchar(128) null
	,server_name varchar(128) null
	,[database_name] varchar(128) null
	,conn_str varchar(max) null
	,file_path varchar(1000) null
	--,api_key varchar(255) null
	--,payload_json varchar(max) null
	,descr varchar(max) null
	,date_created smalldatetime not null default getdate()
	,date_updated smalldatetime null
	,CONSTRAINT metadata_conn_str_PK PRIMARY KEY (conn_str_id)
	,CONSTRAINT metadata_conn_str_UQ UNIQUE (conn_str_name)
)
go
create table metadata.tbl (
    tbl_id int identity not null,
    fully_qualified_tbl_name varchar(255) not null,
	data_source_id int not null CONSTRAINT metadata_tbl_FK1 FOREIGN KEY REFERENCES metadata.data_source(data_source_id),
    incr_date smalldatetime null,
	incr_column varchar(32) null,
    data_size varchar(16) not null default 'small' check (data_size in ('small', 'large')),
    date_created smalldatetime not null default getdate(),
    date_updated smalldatetime null,
    constraint metadata_tbl_PK primary key (tbl_id)
   ,CONSTRAINT metadata_tbl_UQ1 UNIQUE (fully_qualified_tbl_name)
);


go
create table metadata.job(
 job_id int identity not null 
,job_name varchar(128) not null
,is_etl bit not null default 1
,etl_steps varchar(4) not null default 'NONE' check (etl_steps in ('NONE', 'ETL','E','TL'))
,is_full_load bit not null default 1
,del_temp_data bit not null default 1 /* for all tasks to conditionally delete temp table (for small datasets)/CSV file (for large datasets) */
,request_date smalldatetime null
,job_type varchar(4) default 'etl' check (job_type in ('etl', 'non-etl'))
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_job_PK PRIMARY KEY (job_id)
,CONSTRAINT metadata_job_UQ1 UNIQUE (job_name)
)
go


create table metadata.job_task(
 job_task_id int identity not null
,job_id int not null CONSTRAINT metadata_job_task_FK1 FOREIGN KEY REFERENCES metadata.job(job_id)
,is_active bit not null default 1
,job_task_name varchar(128) null
,etl_step varchar(1) null /* values: E or T or L */
,step_seq int not null default 1
--,sql_type varchar(32) null
,sql_text varchar(max) null
,conn_str_id int not null CONSTRAINT metadata_job_task_FK2 FOREIGN KEY REFERENCES metadata.conn_str(conn_str_id)
,src_tbl_id int not null CONSTRAINT metadata_job_task_FK3 FOREIGN KEY REFERENCES metadata.tbl(tbl_id)
,tgt_tbl_id int not null CONSTRAINT metadata_job_task_FK4 FOREIGN KEY REFERENCES metadata.tbl(tbl_id)
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_job_task_PK PRIMARY KEY (job_task_id)
,CONSTRAINT metadata_job_task_UQ UNIQUE (job_id, step_seq)
)
go

create table metadata.job_inst(
 job_inst_id int identity not null 
,job_id int not null CONSTRAINT metadata_job_inst_FK1 FOREIGN KEY REFERENCES metadata.job(job_id)
,etl_steps varchar(4) null
,is_full_load bit not null default 1
,del_temp_data bit not null default 1 /* for all tasks to conditionally delete temp table (for small datasets)/CSV file (for large datasets) */
,job_status varchar(16) not null check (job_status in ('failed', 'running','succeeded')) 
,job_start_date smalldatetime null
,job_end_date smalldatetime null
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_job_inst_PK PRIMARY KEY (job_inst_id)
)
go

create table metadata.job_inst_task(
 job_inst_task_id int identity not null
,job_task_id int not null CONSTRAINT metadata_job_inst_task_FK2 FOREIGN KEY REFERENCES metadata.job_task(job_task_id)
,job_inst_id int not null CONSTRAINT metadata_job_inst_task_FK1 FOREIGN KEY REFERENCES metadata.job_inst(job_inst_id)
,step_seq int not null
,task_start_date smalldatetime null
,task_end_date smalldatetime null
,task_status varchar(16) not null  check (task_status in ('not started', 'failed', 'running','succeeded')) 
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_job_inst_task_PK PRIMARY KEY (job_inst_task_id)
,CONSTRAINT metadata_job_inst_task_UQ UNIQUE (job_inst_id, step_seq)
)

/*
create table metadata.job_scheduler(
 job_scheduler_id int identity not null 
,job_id int not null CONSTRAINT metadata_job_scheduler_FK1 FOREIGN KEY REFERENCES metadata.job(job_id)
,job_inst_id int null 
,job_scheduler_status varchar(16) not null check (job_scheduler_status in ('failed', 'running','succeeded','pending')) 
,job_scheduler_start_date smalldatetime null
,job_scheduler_end_date smalldatetime null
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_job_scheduler_PK PRIMARY KEY (job_scheduler_id)
)
*/
drop table if exists metadata.api_response_log
create table metadata.api_response_log (
    job_id int,
    [timestamp] smalldatetime,
    status_code int,
    response_text nvarchar(max),
    request_url nvarchar(500),
    payload nvarchar(max),
    headers nvarchar(max)
)

drop table if exists metadata.error_log
CREATE TABLE metadata.error_log (
    job_inst_id INT,
    step_name NVARCHAR(50),
    timestamp DATETIME,
    error_message NVARCHAR(MAX),
    context NVARCHAR(MAX)
)

drop table if exists metadata.log_header
CREATE TABLE metadata.log_header (
    log_header_id int identity(1,1) primary key,
    job_inst_id int not null CONSTRAINT metadata_log_dtl_FK2 FOREIGN KEY REFERENCES metadata.job_inst(job_inst_id) ,
	job_name varchar(128) null,
    job_status varchar(20) null,  -- success, failed, started
    start_time smalldatetime null,
    end_time smalldatetime null,
    error_msg varchar(max) null,
    created_at smalldatetime not null default getdate()
);

drop table if exists metadata.log_dtl
CREATE TABLE metadata.log_dtl (
    log_dtl_id int identity(1,1) primary key,
	log_header_id int not null CONSTRAINT metadata_log_dtl_FK1 FOREIGN KEY REFERENCES metadata.log_header(log_header_id),
    task_name varchar(50) null ,
    task_status varchar(20) null,  -- success, failed, skipped
	context varchar(1000) null,
    error_msg varchar(max) null,
	is_error bit not null default 0,
    created_at smalldatetime not null default getdate()
);
go



SELECT TOP (2) * FROM [dmk_stage_db].[metadata].[job_inst_task]  order by 1 desc
SELECT TOP 1 * FROM [dmk_stage_db].[metadata].[job_inst]  order by 1 desc
select top 1 * from [metadata].[log_header] order by 1 desc
select top 10 * from [metadata].[log_dtl] order by 1 desc
