
CREATE OR ALTER PROCEDURE raw.insert_reqres_user (
	@p_job_inst_id int = 0,
	@p_json NVARCHAR(MAX)= null

)
AS
    BEGIN

/********************************************************
*
* Purpose: 
1. JSON --> [raw].reqres_user table
2. [raw].reqres_user table --> [dbo].reqres_user table

*          flatten json using OPENJSON with the WITH clause, 
			and extract both 'data' and 'support' fields into a single row,
			then insert into the table
* Parameters:
*
* Modified:
* 
********************************************************/
    SET NOCOUNT ON;

/* testing:
DECLARE @json NVARCHAR(MAX) = '{
    "data": {
        "id": 2,
        "email": "janet.weaver@reqres.in",
        "first_name": "Janet",
        "last_name": "Weaver",
        "avatar": "https://reqres.in/img/faces/2-image.jpg"
    },
    "support": {
        "url": "https://contentcaddy.io?utm_source=reqres&utm_medium=json&utm_campaign=referral",
        "text": "Tired of writing endless social media content? Let Content Caddy generate it for you."
    }
}';

set @p_json = @json
*/
BEGIN TRY

declare @row_count int
, @info_msg varchar(1000)

	truncate table [raw].reqres_user

    INSERT INTO [raw].reqres_user (
        id,
        email,
        first_name,
        last_name,
        avatar,
        support_url,
        support_text,
		job_inst_id
    )
    SELECT
        data.id,
        data.email,
        data.first_name,
        data.last_name,
        data.avatar,
        support.url,
        support.text,
		@p_job_inst_id
    FROM
        OPENJSON(@p_json, '$.data')
        WITH (
            id INT,
            email NVARCHAR(100),
            first_name NVARCHAR(50),
            last_name NVARCHAR(50),
            avatar NVARCHAR(200)
        ) AS data
    CROSS APPLY
        OPENJSON(@p_json, '$.support')
        WITH (
            url NVARCHAR(200),
            text NVARCHAR(MAX)
        ) AS support;

set @row_count = @@ROWCOUNT
set @info_msg = 'Table [raw].reqres_user || inserted rows || ' + FORMAT(@row_count, 'n0') 
	
		EXEC [metadata].[sp_add_log_dtl]  
                @p_job_inst_id = @p_job_inst_id,
                @p_task_name = 'extract regres user',
                @p_task_status = 'running',
                @p_error_msg = @info_msg ,
                @p_context = 'sp [raw].[insert_reqres_user]',
                @p_is_error = 0


-- step 2:
execute dbo.insert_reqres_user @p_job_inst_id = @p_job_inst_id

END TRY
BEGIN CATCH
	THROW
END CATCH

END;
