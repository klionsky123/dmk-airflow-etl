
CREATE OR ALTER PROCEDURE dbo.insert_reqres_resource (
	@p_job_inst_id int
,	@p_job_inst_task_id int 

)
AS
    BEGIN

/********************************************************
*
* Purpose: 
[raw].reqres_resource table --> [dbo].reqres_resource table

[raw].reqres_resource table - NVARCHAR columns
[dbo].reqres_resource table - correct data types
* Parameters:
*
* Modified:
* 
********************************************************/
    SET NOCOUNT ON;

BEGIN TRY

--Select @p_job_inst_id = Max(job_inst_id) from raw.reqres_resource

declare @row_count int
, @info_msg varchar(1000)

	truncate table [dbo].reqres_resource

    INSERT INTO [dbo].reqres_resource (
		id,
        name,
        year,
        color,
        pantone_value,
        page,
        per_page,
        total,
        total_pages,
        support_url,
        support_text,
        job_inst_id
    )
    SELECT
		id,
        name,
        year,
        color,
        pantone_value,
        page,
        per_page,
        total,
        total_pages,
        support_url,
        support_text,
        job_inst_id
	from [raw].reqres_resource
	where job_inst_id = @p_job_inst_id
 

set @row_count = @@ROWCOUNT
set @info_msg = 'Table [dbo].reqres_resource || inserted rows || ' + FORMAT(@row_count, 'n0') 
	
		EXEC [metadata].[sp_add_log_dtl]  
                @p_job_inst_id = @p_job_inst_id,
                @p_task_name = 'extract regres resource || raw --> dbo',
                @p_task_status = 'running',
                @p_error_msg = @info_msg ,
                @p_context = 'sp [dbo].[insert_reqres_resource]',
                @p_is_error = 0


END TRY
BEGIN CATCH
	THROW
END CATCH

END;
