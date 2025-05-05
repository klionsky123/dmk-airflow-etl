
use dmk_stage_db
go
CREATE OR ALTER PROCEDURE [metadata].[sp_crud_log_header] (
	   @p_action VARCHAR(3) ='INS'
      ,@p_job_inst_id int  = 1
      ,@p_job_status varchar(20) = 'running'
      ,@p_error_msg varchar(max) = null

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
* 
exec [metadata].[sp_crud_log_header] 'INS', 1, 'running', 'started'
exec [metadata].[sp_crud_log_header] 'upd',22, 'succeeded', null
********************************************************/

-- SET NOCOUNT to ON and no longer display the count message.
SET NOCOUNT ON


BEGIN TRY

DECLARE @log_header_id INT

if @p_action = 'INS' BEGIN

declare @job_name varchar(128) =
							(Select j.job_name from [metadata].[job_inst] ji 
							inner join 	[metadata].[job] j on j.job_id = ji.job_id 
							where ji.job_inst_id = @p_job_inst_id)

	insert into  [metadata].[log_header](
		   job_inst_id
		  ,job_name
		  ,job_status
		  ,start_time

	)

	select 
		   job_inst_id = @p_job_inst_id
		  ,job_name	   = @job_name
		  ,job_status	= @p_job_status
		  ,start_time	= GETDATE() 

	SELECT @log_header_id = SCOPE_IDENTITY()

		insert into [metadata].[log_dtl](
			log_header_id
			,task_name
			,task_status
			,error_msg
			,context
			,is_error
			)
		select
			 log_header_id	= @log_header_id
			,task_name		= 'start_job'
			,task_status	= 'started'
			,error_msg		= '*** STARTED || ' + @job_name + ' with job_inst_id = ' + str(@p_job_inst_id)
			,context		= 'sp sp_crud_log_header'
			,is_error		= 0



END
ELSE IF @p_action = 'UPD' BEGIN


	UPDATE [metadata].[log_header]
	SET 
		   job_status	= @p_job_status
		  ,end_time		= case when @p_job_status  in ('failed', 'succeeded') then getdate() else [end_time] end
		  ,error_msg	= @p_error_msg
	WHERE   job_inst_id = @p_job_inst_id

	 


END 

END TRY
BEGIN CATCH
	THROW
END CATCH

    END