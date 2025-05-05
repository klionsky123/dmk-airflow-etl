USE [dmk_stage_db]
GO
/****** Object:  StoredProcedure [dbo].[insert_openalex_tbls]    Script Date: 5/2/2025 10:48:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER       PROCEDURE [dbo].[insert_openalex_tbls] (
	 @p_job_inst_id int
	,@p_job_inst_task_id int 

)
AS
    BEGIN

/********************************************************
*
* Purpose: 
		place holder
		called on Extract
		raw --> dbo tables
* Parameters:
*
* Modified:
* 
********************************************************/
    SET NOCOUNT ON;


BEGIN TRY

declare @info_msg varchar(1000)= ''
	
select @info_msg = 'From raw to dbo || ' + t1.[fully_qualified_tbl_name] + ' --> ' + t2.[fully_qualified_tbl_name]
 
  FROM [metadata].[job_inst_task] jit 
  inner join [metadata].[job_task] jt on jit.job_task_id = jt.job_task_id
  left outer join [metadata].[tbl] t1 on t1.tbl_id = jt.src_tbl_id
  left outer join [metadata].[tbl] t2 on t2.tbl_id = jt.tgt_tbl_id
  

  where jit.job_inst_task_id = @p_job_inst_task_id



		EXEC [metadata].[sp_add_log_dtl]  
                @p_job_inst_id = @p_job_inst_id,
                @p_task_name = 'extract',
                @p_task_status = 'running',
                @p_error_msg = @info_msg ,
                @p_context = 'sp [dbo].[insert_openalex_tbls]',
                @p_is_error = 0



END TRY
BEGIN CATCH
	THROW
END CATCH

END;
