use dmk_stage_db
go
CREATE OR ALTER PROCEDURE dbo.insert_city (
	@p_job_inst_id int
	,@p_job_inst_task_id int
)
AS
    BEGIN

/********************************************************
*
* Purpose: 
[raw].city table --> [dbo].city table

[raw].city table - NVARCHAR columns
[dbo].city table - correct data types
* Parameters:
*
* Modified:
* 
********************************************************/
    SET NOCOUNT ON;

BEGIN TRY

declare @row_count int
, @info_msg varchar(1000)

	truncate table [dbo].city

    INSERT INTO [dbo].city (
	   [city_id]
      ,[name]
      ,[locale_id]
      ,[master_record_id]
      ,[job_inst_id]
    )
    SELECT
	   [city_id]
      ,[name]
      ,[locale_id]
      ,[master_record_id]
      ,@p_job_inst_id
	from [raw].city

set @row_count = @@ROWCOUNT
	
select @info_msg = 'inserted rows || ' + FORMAT(@row_count, 'n0') +' || '+ t1.[fully_qualified_tbl_name] + ' --> ' + t2.[fully_qualified_tbl_name]
 
  FROM [metadata].[job_inst_task] jit 
  inner join [metadata].[job_task] jt on jit.job_task_id = jt.job_task_id
  left outer join [metadata].[tbl] t1 on t1.tbl_id = jt.src_tbl_id
  left outer join [metadata].[tbl] t2 on t2.tbl_id = jt.tgt_tbl_id

	
		EXEC [metadata].[sp_add_log_dtl]  
                @p_job_inst_id = @p_job_inst_id,
                @p_task_name = 'extract-city-step-2 || raw --> dbo',
                @p_task_status = 'running',
                @p_error_msg = @info_msg ,
                @p_context = 'sp [dbo].[insert_city]',
                @p_is_error = 0


END TRY
BEGIN CATCH
	THROW
END CATCH

END;
