use dmk_prod_db
go

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'conformed'
)
BEGIN
    EXEC('CREATE SCHEMA conformed')
END
go



use dmk_stage_db
go

/*
Script to create 'stage' metadata tables.

*/
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
go
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'load'
)
BEGIN
    EXEC('CREATE SCHEMA load')
END

--------------------------
go

drop table if exists metadata.log_dtl
drop table if exists metadata.log_header
drop table if exists metadata.job_inst_task
drop table if exists metadata.job_inst
drop table if exists metadata.job_task
drop table if exists metadata.job
drop table if exists metadata.tbl
drop table if exists metadata.conn_str
drop table if exists metadata.conn_api
drop table if exists  metadata.conn_kafka
drop table if exists metadata.date_source
drop table if exists metadata.sql_script
go
create table metadata.data_source(
 data_source_id int identity not null
,data_source_name varchar(256) not null
,[data_source_type] [varchar](256) NOT NULL default 'db' CHECK ([data_source_type] IN ('api', 'db', 'file', 'kafka')) -- enforce type,
,descr varchar(max) null
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_data_source_PK PRIMARY KEY (data_source_id)
,CONSTRAINT metadata_data_source_UQ UNIQUE (data_source_name)
)

create table metadata.sql_script(
 sql_script_id int identity not null
,is_stored_proc bit null default 1
,sql_text varchar(max) not null
,descr varchar(max) null
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_sql_script_PK PRIMARY KEY (sql_script_id)
)


CREATE TABLE [metadata].[conn_api] (
    conn_api_id INT IDENTITY(1,1) PRIMARY KEY,
	conn_api_name varchar(256) not null CONSTRAINT metadata_conn_api_UQ UNIQUE (conn_api_name),
	data_source_id int not null CONSTRAINT metadata_conn_api_FK1 FOREIGN KEY REFERENCES metadata.data_source(data_source_id),

	-- foreign key to a parent conn_api (if token must be fetched)
	parent_conn_api_id INT NULL CONSTRAINT metadata_conn_api_FK2 FOREIGN KEY REFERENCES metadata.conn_api(conn_api_id),  
    api_url VARCHAR(1024) NOT NULL,
	http_method varchar(16) null,

    -- API Key or Token
    api_key VARCHAR(255) NULL,
    need_token BIT NOT NULL DEFAULT 0,
    username VARCHAR(128) NULL,
    pass VARCHAR(128) NULL,

    -- Optional JSON payload (for POST calls or static params)
    payload_json VARCHAR(MAX) NULL,

    -- Pagination handling
    pagination_type VARCHAR(50) NULL, -- e.g. 'page', 'offset', 'cursor', 'none'
    page_param_name VARCHAR(50) NULL DEFAULT 'page',
    per_page_param_name VARCHAR(50) NULL DEFAULT 'per_page',
    page_size INT NULL DEFAULT 100,
	max_pages int null,
	output_file_path varchar(1000) null,
	chunk_size int null,

    -- Cursor pagination support
    cursor_param_name VARCHAR(50) NULL,
    cursor_path VARCHAR(255) NULL, -- JSON path to extract next cursor from response

    -- Filters or incremental sync
    use_incremental BIT NOT NULL DEFAULT 0,
    modified_since_param VARCHAR(50) NULL,
    last_sync_time smalldatetime NULL,

    descr VARCHAR(255) NULL,
	date_created smalldatetime not null default getdate(),
	date_updated smalldatetime null
)
go


create table metadata.conn_kafka(
	 conn_kafka_id int identity not null
	,topic varchar(256) not null		/* Kafka topic to consume messages from */
	,topic_pattern varchar(256)  null		/* Kafka topic patter to subscribe e.g. test-.* */
	,data_source_id int not null CONSTRAINT metadata_conn_kafka_FK2 FOREIGN KEY REFERENCES metadata.data_source(data_source_id)
	,bootstrap_servers varchar(512) not null /* List of Kafka brokers (e.g., localhost:9092) */
	,group_id varchar(128) not null /* Consumer group ID for Kafka offset tracking */
	,batch_record_size int null    /* Number of records to buffer before writing to the database (default is 100), for consumer */
	,max_message_num int null default 1000 /* # Set limit if we don’t want to consume forever */
	,is_consumer bit not null default 1 /* consumer or producer */
	,descr varchar(max) null
	,date_created smalldatetime not null default getdate()
	,date_updated smalldatetime null
	,CONSTRAINT metadata_conn_kafka_PK PRIMARY KEY (conn_kafka_id)
	,CONSTRAINT metadata_conn_kafka_UQ UNIQUE (topic, is_consumer)
)
go

create table metadata.conn_str(
	 conn_str_id int identity not null
	,conn_str_name varchar(256) not null
	,parent_conn_str_id int null		/* for api key dependency */
	,data_source_id int not null CONSTRAINT metadata_conn_str_FK2 FOREIGN KEY REFERENCES metadata.data_source(data_source_id)
	,username varchar(128) null
	,pass varchar(128) null
	,server_name varchar(128) null
	,[database_name] varchar(128) null
	,conn_str varchar(max) null
	,file_path varchar(1000) null
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
/*
Airflow Tagging Convention

Tag Type		Example							Description
------------------------------------------------------------------------------------------
System			etl, api, ml, dbt, spark		What kind of workload or tool the DAG uses.
Data Domain		finance, sales, hr, customer	What area of the business the DAG supports.
Environment		dev, staging, prod				Deployment stage — important for managing test vs live runs.
Priority		critical, daily, adhoc			How important/frequent the DAG is.
Team/Owner		team-data, team-ml, team-bi		Helps identify responsibility for the DAG.

*/
create table metadata.job(
 job_id int identity not null 
,job_name varchar(128) not null
,job_group_name varchar(128) null
,job_group_seq int null 
,is_etl bit not null default 1
,etl_steps varchar(4) not null default 'NONE' check (etl_steps in ('NONE', 'ETL','E','TL'))
,is_full_load bit not null default 1
,del_temp_data bit not null default 1 /* for all tasks to conditionally delete temp table (for small datasets)/CSV file (for large datasets) */
,job_type varchar(8) NULL default 'etl' check (job_type in ('etl', 'non-etl'))
,is_active bit not null default 1
,airflow_schedule varchar(64) not null default '0 9 * * *'
,airflow_tags varchar(32) null
,send_notification bit default 0
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
,etl_step varchar(4) not null default 'NONE' /* values: E or T or L */
,step_seq int not null default 1
,sql_script_id int null CONSTRAINT metadata_job_task_FK6 FOREIGN KEY REFERENCES metadata.sql_script(sql_script_id)
,conn_api_id int null CONSTRAINT metadata_job_task_FK5 FOREIGN KEY REFERENCES metadata.conn_api(conn_api_id)
,conn_kafka_id int null CONSTRAINT metadata_job_task_FK7 FOREIGN KEY REFERENCES metadata.conn_kafka(conn_kafka_id)
,conn_type varchar(16) not null default 'db' check (conn_type in ('db', 'api','file','kafka') )
,conn_str_id int null CONSTRAINT metadata_job_task_FK2 FOREIGN KEY REFERENCES metadata.conn_str(conn_str_id)
,src_tbl_id int not null CONSTRAINT metadata_job_task_FK3 FOREIGN KEY REFERENCES metadata.tbl(tbl_id)
,tgt_tbl_id int not null CONSTRAINT metadata_job_task_FK4 FOREIGN KEY REFERENCES metadata.tbl(tbl_id)
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_job_task_PK PRIMARY KEY (job_task_id)
,CONSTRAINT metadata_job_task_UQ UNIQUE (job_id, etl_step,step_seq)
)
go

create table metadata.job_inst(
 job_inst_id int identity not null 
,job_id int not null CONSTRAINT metadata_job_inst_FK1 FOREIGN KEY REFERENCES metadata.job(job_id)
,etl_steps varchar(4) null
,is_full_load bit not null default 1
,del_temp_data bit not null default 1 /* for all tasks to conditionally delete temp table (for small datasets)/CSV file (for large datasets) */
,job_status varchar(16) not null check (job_status in ('failed', 'running','succeeded', 'started')) 
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
,etl_step varchar(4) not null /* values: E or T or L */
,step_seq int not null
,task_start_date smalldatetime null
,task_end_date smalldatetime null
,task_status varchar(16) not null  check (task_status in ('not started', 'failed', 'running','succeeded')) 
,date_created smalldatetime not null default getdate()
,date_updated smalldatetime null
,CONSTRAINT metadata_job_inst_task_PK PRIMARY KEY (job_inst_task_id)
,CONSTRAINT metadata_job_inst_task_UQ UNIQUE (job_inst_id, etl_step, step_seq)
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
	logging_seq int null,
	is_error bit not null default 0,
    created_at datetime not null default getdate()
);
go



SELECT TOP (2) * FROM [dmk_stage_db].[metadata].[job_inst_task]  order by 1 desc
SELECT TOP 1 * FROM [dmk_stage_db].[metadata].[job_inst]  order by 1 desc
select top 1 * from [metadata].[log_header] order by 1 desc
select top 10 * from [metadata].[log_dtl] order by 1 desc
