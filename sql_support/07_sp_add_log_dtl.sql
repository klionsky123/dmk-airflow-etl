
use dmk_stage_db
go
CREATE OR ALTER PROCEDURE [metadata].[sp_add_log_dtl] (
			   @p_job_inst_id int = 1
			  ,@p_task_name varchar(50) ='extract'
			  ,@p_task_status varchar(20) ='failed'
			  ,@p_error_msg varchar(max) ='test'
			  ,@p_context varchar(1000) ='test'
			  ,@p_is_error bit = 1

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
exec [metadata].[sp_add_log_dtl] 16, 'extact', 'failed', 'test', 'test',1

********************************************************/

-- SET NOCOUNT to ON and no longer display the count message.
SET NOCOUNT ON


BEGIN TRY

insert into [metadata].[log_dtl](
	log_header_id
	,task_name
	,task_status
	,error_msg
	,context
	,is_error

)
select
	 log_header_id	= (select log_header_id from [metadata].[log_header] where job_inst_id = @p_job_inst_id)
	,task_name		= @p_task_name
	,task_status	= @p_task_status
	,error_msg		= @p_error_msg
	,context		= @p_context
	,is_error		= @p_is_error

if @p_task_status = 'failed' BEGIN

	exec [metadata].[sp_crud_log_header] 'upd',@p_job_inst_id, 'failed', @p_error_msg

END

END TRY
BEGIN CATCH
	THROW
END CATCH

    END