
use dmk_stage_db
go
CREATE OR ALTER PROCEDURE [metadata].[sp_crud_job_inst_task] (
    @p_action varchar(3) = 'INS', 
	@p_job_inst_task_id int = 0,
	@p_task_status varchar(16) =NULL,
	@p_job_inst_id int = 0,
	@p_step_seq int = 0,
	@p_job_task_id int = 0

)
AS
    BEGIN

/********************************************************
*
* Purpose: crud operations for [metadata].[job_inst]
*          
* Parameters:
*
* Modified:
* 
exec [metadata].[sp_crud_job_inst_task] 'INS', null,null, 2,1, null
exec [metadata].[sp_crud_job_inst_task] 'upd', 42, 'failed'
********************************************************/

-- SET NOCOUNT to ON and no longer display the count message.
SET NOCOUNT ON


BEGIN TRY
--select:
if @p_action='INS' BEGIN 

	INSERT INTO [metadata].[job_inst_task](
		   [job_inst_id]
		  ,[job_task_id]
		  ,etl_step
		  , step_seq
		  ,[task_start_date]
		  ,[task_end_date]
		  ,[task_status]
	)
	SELECT 
		   [job_inst_id]		= @p_job_inst_id
		  ,[job_task_id]		= @p_job_task_id
		  ,etl_step				= (select etl_step from [metadata].[job_task] where job_task_id = @p_job_task_id)
		  ,step_seq				= @p_step_seq
		  ,[task_start_date]	= NULL
		  ,[task_end_date]		= NULL
		  ,[task_status]		= 'not started'

	SELECT SCOPE_IDENTITY()

END
ELSE if @p_action = 'UPD' BEGIN

	UPDATE  metadata.job_inst_task
		set [task_status] = @p_task_status
			,[task_start_date] = case when @p_task_status ='running' then GETDATE() else [task_start_date] end
			,[task_end_date]	= case when @p_task_status  in ('failed', 'succeeded') then getdate() else [task_end_date] end
			, date_updated = getdate()
		where (@p_job_inst_id <> 0 and @p_job_inst_task_id =0 and (job_inst_id = @p_job_inst_id and step_seq = @p_step_seq))
			  or
			  (@p_job_inst_id = 0 and @p_job_inst_task_id <> 0 and (job_inst_task_id = @p_job_inst_task_id))

END
ELSE if @p_action = 'SEL' BEGIN

SELECT [job_inst_task_id]
    ,[job_task_id]
    ,[job_inst_id]
	,etl_step
    ,[step_seq]
    ,[task_start_date]
    ,[task_end_date]
    ,[task_status]
    ,[date_created]
    ,[date_updated]
FROM [metadata].[job_inst_task]
where (@p_job_inst_id <> 0 and @p_job_inst_task_id =0 and (job_inst_id = @p_job_inst_id and step_seq = @p_step_seq))
		or
		(@p_job_inst_id = 0 and @p_job_inst_task_id <> 0 and (job_inst_task_id = @p_job_inst_task_id))


END
  

END TRY
BEGIN CATCH
	THROW
END CATCH

    END