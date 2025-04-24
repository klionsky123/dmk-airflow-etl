use dmk_stage_db
go

CREATE OR ALTER PROCEDURE [metadata].[sp_get_job_inst_task] (
	@p_job_inst_id int = 0,
	@p_etl_step varchar(1) = 'E' /* values: E or T or L */


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
* execute [metadata].[sp_get_job_inst_task] 126, 'E'
********************************************************/

-- SET NOCOUNT to ON and no longer display the count message.
SET NOCOUNT ON


BEGIN TRY

SELECT jit.[job_inst_task_id]
      ,jt.[job_id]
      ,jt.[job_task_name]
      ,jt.[etl_step]
      ,jt.[step_seq]
      ,[sql_type] = case when cst.conn_str_type_name in ('mssql-stored-proc','postgres-stored-proc') then 'proc' 
						 when cst.conn_str_type_name in ('mssql-raw-sql', 'postgres-raw-sql') then 'raw-sql' 
						 when cst.conn_str_type_name = 'api-get' then 'get'
						 when cst.conn_str_type_name = 'api-post' then 'post'
 						 else null end
      ,jt.[sql_text]

	  /* connection string info: */
	  ,cs.[conn_str_name]
	  ,conn_type = cst.conn_str_type_name
      ,cs.[username]
      ,cs.[pass]
	  /* "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server" */
      /*,[conn_str] = 'mssql+pyodbc://' + [username] + ':' + pass + '@' + [server_name] + '/' + [database_name] + '?driver=ODBC+Driver+17+for+SQL+Server' */
	  ,cs.[conn_str]
      ,cs.[file_path]
      ,cs.[need_token]
      ,cs.[payload_json]

	  /* tbl info: */
      ,[src_fully_qualified_tbl_name] = t1.[fully_qualified_tbl_name]
      ,[tgt_fully_qualified_tbl_name] = t2.[fully_qualified_tbl_name]
	  ,src_incr_date = t1.[incr_date]
      ,src_data_size = t1.[data_size]
	  ,jit.task_status

	  /* api key info */
	  ,api_key_url = case when cs.parent_conn_str_id is not null then (select conn_str from [metadata].[conn_str] where conn_str_id = cs.parent_conn_str_id) else null end
	  ,api_key_username = case when cs.parent_conn_str_id is not null then (select [username] from [metadata].[conn_str] where conn_str_id = cs.parent_conn_str_id) else null end
	  ,api_key_pass = case when cs.parent_conn_str_id is not null then (select [pass] from [metadata].[conn_str] where conn_str_id = cs.parent_conn_str_id) else null end

  FROM [metadata].[job_inst_task] jit 
  inner join [metadata].[job_task] jt on jit.job_task_id = jt.job_task_id
  left outer join [metadata].[tbl] t1 on t1.tbl_id = jt.src_tbl_id
  left outer join [metadata].[tbl] t2 on t2.tbl_id = jt.tgt_tbl_id
  left outer join [metadata].[conn_str] cs on jt.[conn_str_id] = cs.conn_str_id
  left outer join [metadata].[conn_str_type] cst on cs.conn_str_type_id = cst.conn_str_type_id
  where jit.job_inst_id = @p_job_inst_id
  and jt.etl_step = @p_etl_step
  order by step_seq
  

END TRY
BEGIN CATCH
	THROW
END CATCH

    END