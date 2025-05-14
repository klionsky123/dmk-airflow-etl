use dmk_stage_db
go

CREATE OR ALTER PROCEDURE [metadata].[sp_get_job_inst_task] (
	@p_job_inst_id int ,
	@p_etl_step varchar(4) = 'E', /* values: E or T or L , NONE - for non-etl jobs */
	@p_job_inst_task_id int = 0


)
AS
    BEGIN

/********************************************************
*
* Purpose: 
*          
* Parameters:
*
* Modified:

* Example of usage:
* execute [metadata].[sp_get_job_inst_task] 428, 'e',0
********************************************************/

-- SET NOCOUNT to ON and no longer display the count message.
SET NOCOUNT ON

PRINT @p_etl_step

BEGIN TRY

SELECT jit.[job_inst_task_id]
      ,jt.[job_id]
      ,jt.[job_task_name]
      ,jt.[etl_step]
      ,jt.[step_seq]
      ,[sql_type] = case when ss.is_stored_proc = 1 then 'proc' 
						 when ss.is_stored_proc = 0 then 'raw-sql' 
 						 else null end
      ,ss.[sql_text]

	  /* connection string info: */
	  ,conn_str_name = case when jt.conn_type ='db' then cs.[conn_str_name] else api.conn_api_name end
	  ,conn_type = jt.conn_type
      ,cs.[username]
      ,cs.[pass]
	  ,cs.file_path
	  /* "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server" */
      /*,[conn_str] = 'mssql+pyodbc://' + [username] + ':' + pass + '@' + [server_name] + '/' + [database_name] + '?driver=ODBC+Driver+17+for+SQL+Server' */
	  ,conn_str =  case when jt.conn_type ='db' then cs.[conn_str] else api.api_url end 
      ,cs.[file_path]
      ,api.[need_token]
      ,api.[payload_json]
	  ,db_type = case when jt.conn_type ='db' and (conn_str like '%mssql%' or conn_str like '%SQL Server%') then 'MSSQL' 
				      when jt.conn_type ='db' and conn_str like '%postgres%' then 'POSTGRES'
					  else null end

	  /* tbl info: */
      ,[src_fully_qualified_tbl_name] = t1.[fully_qualified_tbl_name]
      ,[tgt_fully_qualified_tbl_name] = t2.[fully_qualified_tbl_name]
	  ,src_incr_date = isnull(t1.[incr_date], getdate() - 1)
	  ,src_incr_column = t1.[incr_column]
      ,src_data_size = t1.[data_size]
	  ,jit.task_status

	  /* api key info */
	  ,api_key_url = case when api.parent_conn_api_id is not null then (select api_url from [metadata].[conn_api] where conn_api_id = api.parent_conn_api_id) else null end
	  ,api_key_username = case when api.parent_conn_api_id is not null then (select [username] from [metadata].[conn_api] where conn_api_id = api.parent_conn_api_id) else null end
	  ,api_key_pass = case when api.parent_conn_api_id is not null then (select pass from [metadata].[conn_api] where conn_api_id = api.parent_conn_api_id) else null end

	  /* cursor pagination */
	  ,api.[page_param_name]
	  ,api.[page_size]
	  ,api.[cursor_param_name]
	  ,api.[cursor_path]
	  ,api.[max_pages]
	  ,api.[output_file_path]
	  ,api.[chunk_size]

	  ,ds.data_source_name
--	  ,ds.[data_source_type] is the same as jt.conn_type
	  ,ji.[job_inst_id]
	  ,ji.[etl_steps]
	  ,ji.[is_full_load]
	  ,ji.[del_temp_data]
	  ,j.job_type

	  /*kafka related */
	   ,kafka_topic = kafka.topic
	   /* Docker Networking: By default, Docker Compose creates a network where services can reach each other by container name. 
	      So, from the Airflow container, you should connect to kafka:9092, not localhost:9092  */
       ,kafka_bootstrap_servers=  kafka.bootstrap_servers /* 'kafka:9092'  and theses do not work : 'localhost:9092' or '127.0.0.1:9092' */
       ,kafka_group_id= kafka.group_id  /* e.g. 'etl-group' */
	   ,is_kafka_consumer =kafka.is_consumer /*consumer or producer */
	   ,kafka_batch_record_size = kafka.batch_record_size
	   ,kafka.max_message_num
	   ,kafka_topic_pattern = kafka.topic_pattern

  
  FROM [metadata].[job_inst_task] jit 
  left outer join [metadata].[job_task] jt on jit.job_task_id = jt.job_task_id
  left outer join [metadata].[job_inst] ji on ji.job_inst_id = jit.job_inst_id
  left outer join [metadata].[job] j on ji.job_id = j.job_id
  left outer join [metadata].[tbl] t1 on t1.tbl_id = jt.src_tbl_id
  left outer join [metadata].[tbl] t2 on t2.tbl_id = jt.tgt_tbl_id
  left outer join [metadata].[conn_str] cs on jt.[conn_str_id] = cs.conn_str_id
  left outer join [metadata].[conn_api] api on jt.[conn_api_id] = api.[conn_api_id]
  left outer join [metadata].[conn_kafka] kafka on jt.[conn_kafka_id] = kafka.[conn_kafka_id]
  left outer join [metadata].[sql_script] ss on jt.[sql_script_id] = ss.sql_script_id
  left outer join [metadata].[data_source] ds on api.data_source_id =ds.data_source_id

  where jit.job_inst_id = @p_job_inst_id
  and jt.etl_step = @p_etl_step
  and ((@p_job_inst_task_id = 0)
	   or
	   (@p_job_inst_task_id != 0 and jit.job_inst_task_id = @p_job_inst_task_id))
  order by step_seq
  

END TRY
BEGIN CATCH
	THROW
END CATCH

    END