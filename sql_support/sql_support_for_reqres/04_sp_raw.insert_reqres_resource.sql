
CREATE OR ALTER PROCEDURE raw.insert_reqres_resource (
	@p_job_inst_id int = 0,
	@p_json NVARCHAR(MAX)= null

)
AS
    BEGIN

/********************************************************
*
* Purpose: 
1. JSON --> [raw].reqres_resource table
2. [raw].reqres_resource table --> [dbo].reqres_resource table

*          flatten 2-level @json using OPENJSON with the WITH clause, 
			and extract both 'data' and 'support' fields into a single row,
			then insert into the table
* Parameters:
*
* Modified:
* 
********************************************************/
    SET NOCOUNT ON;


BEGIN TRY

declare @row_count int
, @info_msg varchar(1000)

	truncate table [raw].reqres_resource

    INSERT INTO [raw].reqres_resource (
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
        data.id,
        data.name,
        data.year,
        data.color,
        data.pantone_value,
        meta.page,
        meta.per_page,
        meta.total,
        meta.total_pages,
        support.url,
        support.text,
        @p_job_inst_id
    FROM
        OPENJSON(@p_json, '$.data')
        WITH (
            id INT,
            name NVARCHAR(100),
            year INT,
            color NVARCHAR(50),
            pantone_value NVARCHAR(50)
        ) AS data
    CROSS APPLY
        OPENJSON(@p_json)
        WITH (
            page INT,
            per_page INT,
            total INT,
            total_pages INT
        ) AS meta
    CROSS APPLY
        OPENJSON(@p_json, '$.support')
        WITH (
            url NVARCHAR(200),
            text NVARCHAR(MAX)
        ) AS support;

set @row_count = @@ROWCOUNT
set @info_msg = 'Table [raw].reqres_resource || inserted rows || ' + FORMAT(@row_count, 'n0') 
	
		EXEC [metadata].[sp_add_log_dtl]  
                @p_job_inst_id = @p_job_inst_id,
                @p_task_name = 'extract regres resource',
                @p_task_status = 'running',
                @p_error_msg = @info_msg ,
                @p_context = 'sp [raw].[insert_reqres_resource]',
                @p_is_error = 0


-- step 2:
execute dbo.insert_reqres_resource @p_job_inst_id = @p_job_inst_id

END TRY
BEGIN CATCH
	THROW
END CATCH

END;
