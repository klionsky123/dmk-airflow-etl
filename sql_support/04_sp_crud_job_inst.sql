CREATE OR ALTER PROCEDURE [metadata].[sp_crud_job_inst] (
    @p_action varchar(3) = 'INS', 
	@p_job_id int = 0,
	@p_job_inst_id int = 0,
	@p_job_status varchar(16) =NULL

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
exec [metadata].[sp_crud_job_inst] 'INS', 1
exec [metadata].[sp_crud_job_inst] 'upd',null, 21, 'succeeded'
exec [metadata].[sp_crud_job_inst] 'SEL',0, 10, NULL
********************************************************/

-- SET NOCOUNT to ON and no longer display the count message.
SET NOCOUNT ON

Declare @job_inst_id int -- to return after insert
, @error_msg varchar(max) = null
,@etl_steps varchar(3)
,@is_etl bit

BEGIN TRY
--select:
if @p_action='INS' BEGIN 

select 
 @etl_steps = etl_steps
,@is_etl = is_etl
from metadata.job 
where job_id = @p_job_id

INSERT INTO metadata.job_inst(job_id, etl_steps, is_full_load,del_temp_data, job_status, job_start_date)
select 
	  job_id		= @p_job_id
	, etl_steps		= (select etl_steps from metadata.job where job_id = @p_job_id)
	, is_full_load	= (select is_full_load from metadata.job where job_id = @p_job_id)
	, del_temp_data = (select del_temp_data  from metadata.job where job_id = @p_job_id)
	, job_status	= 'running'
	, job_start_date= getdate()

SELECT @job_inst_id = SCOPE_IDENTITY()

-- copy only those ETL tasks that are part of the current job instance:
if @is_etl = 1   -- check if this is ETL job
begin

	create table #tbl(etl_step varchar(1) primary key)

	if @etl_steps = 'E'
		INSERT into #tbl SELECT 'E'
	else if @etl_steps = 'ETL'
		INSERT into #tbl SELECT 'E' UNION ALL SELECT 'T' UNION ALL SELECT 'L'
	else if @etl_steps = 'TL'
		INSERT into #tbl SELECT 'T' UNION ALL SELECT 'L'
end

-- copy all tasks for this job into job_inst_task table:
	INSERT INTO [metadata].[job_inst_task](
		   [job_inst_id]
		  ,[job_task_id]
		  , etl_step
		  , step_seq
		  ,[task_start_date]
		  ,[task_end_date]
		  ,[task_status]
	)
	SELECT 
		   [job_inst_id]		= @job_inst_id
		  ,[job_task_id]
		  ,etl_step
		  ,[step_seq]				
		  ,[task_start_date]	= NULL
		  ,[task_end_date]		= NULL
		  ,[task_status]		= 'not started'
	from [metadata].[job_task]
	where job_id = @p_job_id
	and (
			(@is_etl = 1 and etl_step in (select etl_step from #tbl))
			or
		    @is_etl =0
	)
	and is_active = 1 /* only active tasks! */

-- add log header:
	exec [metadata].[sp_crud_log_header] 'INS', @job_inst_id, @p_job_status, 'started'


	select @job_inst_id

END
ELSE if @p_action = 'UPD' BEGIN

	UPDATE  metadata.job_inst
		  set job_status = @p_job_status
			, job_end_date = case when @p_job_status in ('failed', 'succeeded') then getdate() else job_end_date end
			, date_updated = getdate()
		where job_inst_id = @p_job_inst_id

	-- get error message if failed:
	if @p_job_status ='failed' begin
		set @error_msg = (select top 1 dtl.error_msg 
							from [metadata].[log_dtl] dtl inner join [metadata].[log_header] h on dtl.[log_header_id] = h.[log_header_id]
							where h.[job_inst_id] = @p_job_inst_id and dtl.is_error = 1)
										
	end

	-- update log header
	exec [metadata].[sp_crud_log_header] 'UPD', @p_job_inst_id, @p_job_status, @error_msg
	--exec [metadata].[sp_crud_log_header] 'upd',22, 'succeeded', null

END
ELSE if @p_action = 'SEL' BEGIN

	SELECT  ji.job_id, job_inst_id, ji.is_full_load, ji.del_temp_data,
			  run_extract = case when ji.etl_steps like '%E%' then 1 else 0 end
			, run_transform = case when ji.etl_steps like '%T%' then 1 else 0 end
			, run_load = case when ji.etl_steps like '%L%' then 1 else 0 end
			, ji.etl_steps
			, [job_status]
			, j.job_name
		FROM  metadata.job_inst ji 
		inner join [metadata].[job] j on j.job_id = ji.job_id
		where job_inst_id = @p_job_inst_id

END
  

END TRY
BEGIN CATCH
	THROW
END CATCH

    END