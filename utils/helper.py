
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

"""
Connection to the metadata server
"""
def get_engine_for_metadata():
    pwd = "demopass" 
    uid = "etl" 
    driver = "{ODBC Driver 17 for SQL Server}"
    server = "192.168.86.96"
    database = "dmk_stage_db"
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    return create_engine(connection_url)

"""
Write to db log
"""
def write_to_log(job_inst_id: int, task_name:str, task_status:str, error_message:str, context:str, is_error: bool):
    engine = get_engine_for_metadata()
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            connection.execute(
                text("""
                        EXEC [metadata].[sp_add_log_dtl]  
                            @p_job_inst_id = :param1,
                            @p_task_name = :param2,
                            @p_task_status = :param3,
                            @p_error_msg = :param4,
                            @p_context = :param5,
                            @p_is_error = :param6
                    """),
                    {"param1": job_inst_id, "param2": task_name, "param3": task_status, "param4": error_message, "param5": context,"param6": is_error}
                )
            trans.commit()
        
        except Exception as e:
            # Rollback the transaction in case of an error
            trans.rollback()
            raise Exception(f"Transaction failed: {e}")


def log_error(job_inst_id: int, task_name:str, error_message:str, context:str):
    write_to_log(job_inst_id, task_name, "failed", error_message, context, True)
      
def log_info(job_inst_id: int, task_name:str, info_message:str, context:str, task_status: str ="running"):
    write_to_log(job_inst_id, task_name, task_status, info_message, context, False)
        
def log_job_task(job_inst_task_id: int, task_status:str):
    """
        update metadata.job_inst_task with the status
        'failed', 'running','succeeded'
    """
    
    engine = get_engine_for_metadata()
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            connection.execute(
                text("""
                        EXEC [metadata].[sp_crud_job_inst_task]  
                            @p_action = :param1,
                            @p_job_inst_task_id = :param2,
                            @p_task_status = :param3
                    """),
                    {"param1": "UPD", "param2": job_inst_task_id, "param3": task_status}
                )
            trans.commit()
        
        except Exception as e:
            # Rollback the transaction in case of an error
            trans.rollback()
            raise Exception(f"Transaction failed: {e}")

def create_job_inst_and_log(job_id: int) -> int:
    """
        # call [metadata].[sp_crud_job_inst] stored proc to:
        # 1. create job_inst_id and related tasks
        # 2. log process start
    """
    engine = get_engine_for_metadata()

    with engine.connect() as connection:
        trans = connection.begin()
        try:
            result = connection.execute(
                text("""
                    EXEC [metadata].[sp_crud_job_inst] 
                        @p_action = :param1,
                        @p_job_id = :param2
                """),
                {"param1": "INS", "param2": job_id}
            ).fetchone()
            trans.commit()

            if result:
                job_inst_id = result[0]  # this returns a single integer (job_inst_id)
                print(f"[{job_inst_id}] Status set to running")
                return job_inst_id
            else:
                raise Exception("Failed to create job_inst_id via [metadata].[sp_crud_job_inst] stored proc.")
        except Exception as e:
            trans.rollback()
            raise Exception(f"Transaction failed: {e}")

def complete_job(job_inst_id: int, success: bool = True):
    """
        # call [metadata].[sp_crud_job_inst] stored proc to:
        # log process end  
    """
    
    engine = get_engine_for_metadata()
    new_status = "succeeded" if success else "failed"

    with engine.connect() as connection:
        trans = connection.begin()
        try:
            connection.execute(
                text("""
                    EXEC [metadata].[sp_crud_job_inst] 
                        @p_action = :param1,
                        @p_job_id = :param2,
                        @p_job_inst_id = :param3,
                        @p_job_status = :param4
                """),
                {
                    "param1": "UPD",
                    "param2": 0,
                    "param3": job_inst_id,
                    "param4": new_status
                }
            )
            trans.commit()
        
        except Exception as e:
            # Rollback the transaction in case of an error
            trans.rollback()
            raise Exception(f"Transaction failed: {e}")


def get_all_job_inst_tasks(job_inst_id: int, etl_step: str):
    """
    # Get all tasks for the current job instance
    * a generator function:  yield each row as a dictionary instead of building and returning a list.
    # @p_etl_step ="E", for extract
    """
    engine = get_engine_for_metadata()

    with engine.connect() as connection:
        try:
            # Get all tasks for the current job instance:
            # @p_etl_step ="E", for extract
            result = connection.execute(
                text("""
                        EXEC [metadata].[sp_get_job_inst_task] 
                            @p_job_inst_id = :param1,
                            @p_etl_step = :param2
                            """),
                {"param1": job_inst_id, "param2": etl_step}
            )

            # rows = result.fetchall()
            # return [dict(row) for row in rows]  # Make rows accessible by column name

            for row in result:
                yield dict(row)  # 👈 Yield one row at a time

        except Exception as e:
            # Rollback the transaction in case of an error
            raise Exception(f"Transaction failed: {e}")

"""
# Get a job task specifics:
# @p_etl_step ="E", for extract
"""
def get_job_inst_task_info(job_inst_id: int, etl_step: str, job_inst_task_id: int):
    engine = get_engine_for_metadata()
    with engine.connect() as connection:
        try:
            # Get all tasks for the current job instance:
            # @p_etl_step ="E", for extract
            result = connection.execute(
                text("""
                        EXEC [metadata].[sp_get_job_inst_task] 
                            @p_job_inst_id = :param1,
                            @p_etl_step = :param2,
                            @p_job_inst_task_id = :param3
                            """),
                {"param1": job_inst_id, "param2": etl_step, "param3": job_inst_task_id}
            )

            row = result.fetchone()
            return dict(row)

        except Exception as e:
            # Rollback the transaction in case of an error
            raise Exception(f"Transaction failed: {e}")

def get_job_inst_info(job_inst_id: int)->dict:
    """
        # call [metadata].[sp_crud_job_inst]  stored proc to:
        # get job parameters  from metadata tables
    """
    engine = get_engine_for_metadata()
    with engine.connect() as connection:
        result = connection.execute(
            text("""
                EXEC [metadata].[sp_crud_job_inst] 
                     @p_action = :param1,
                     @p_job_id = :param2,
                     @p_job_inst_id = :param3,
                     @p_job_status = :param4
            """),
            {
                "param1": "SEL",
                "param2": 0,
                "param3": job_inst_id,
                "param4": None,
            }
        )
        record = result.fetchone()  # List of tuples
        #return [dict(row) for row in records]
        # Fetch the first record and return as a dictionary

        if record:
            data = dict(record)
            job_name = data["job_name"]
            del_temp_data =data['del_temp_data']
            etl_steps = data['etl_steps']
            is_full_load = data['is_full_load']
            log_info(job_inst_id, 'fetch_job_params'
                     , f"Starting Job {job_name} with ETL steps as || {etl_steps} || and full load set to || {is_full_load} "
                     , context="get_job_inst_info()")
            return data
        else:
            log_error(
                job_inst_id=job_inst_id,
                task_name="fetch_job_params",
                error_message=f"No job parameters found for job_inst_id: {job_inst_id}",
                context="fetch_job_params()"
            )
            raise Exception(f"No job parameters found for job_inst_id: {job_inst_id}")


