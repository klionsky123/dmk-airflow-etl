
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from airflow.hooks.base import BaseHook
import inspect
from typing import List, Dict

"""
Connection to the metadata server
"""
def get_engine_for_metadata():
    conn = BaseHook.get_connection("dmk-stage-db-id")
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
                        f"SERVER={conn.host};" \
                        f"DATABASE={conn.schema};" \
                        f"UID={conn.login};" \
                        f"PWD={conn.password}"
    # connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    return create_engine(connection_url)



"""
Write to db log
"""
def write_to_log(job_inst_id: int, task_name:str, task_status:str,
                 error_message:str, context:str, is_error: bool, logging_seq: int ):
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
                            @p_is_error = :param6,
                            @p_logging_seq = :param7
                    """),
                    {"param1": job_inst_id, "param2": task_name, "param3": task_status,
                                "param4": error_message, "param5": context,"param6": is_error, "param7": logging_seq}
                )
            trans.commit()
        
        except Exception as e:
            # Rollback the transaction in case of an error
            trans.rollback()
            raise Exception(f"Transaction failed: {e}")


def log_error(job_inst_id: int, task_name:str, error_message:str
              , context:str, logging_seq: int = 1):
    write_to_log(job_inst_id, task_name, "failed", error_message, context, True, logging_seq)
      
def log_info(job_inst_id: int, task_name:str, info_message:str
             , context:str, task_status: str ="running", logging_seq: int =1):
    write_to_log(job_inst_id, task_name, task_status, info_message, context,  False, logging_seq)
        
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

def get_etl_jobs_list() -> List[Dict]:
    """
    # Return all jobs  as a list of dictionaries
    """
    engine = get_engine_for_metadata()

    with engine.connect() as connection:
        try:
            result = connection.execute(
                text("""
                    EXEC [metadata].[sp_get_job_list] 
                        @p_job_id = :param1,
                        @p_job_type = :param2
                """),
                {"param1": 0, "param2": "ETL"}
            )

            return [dict(row) for row in result]  # 👈 Fully materialize and return list

        except Exception as e:
            raise Exception(f"Transaction failed: {e}")

def get_job_config(job_name: str) -> Dict:
    """
    # Return job's DAG configuration
    """
    engine = get_engine_for_metadata()

    with engine.connect() as connection:
        try:
            result = connection.execute(
                text("""
                    EXEC [metadata].[sp_get_job_by_name] 
                        @p_job_name = :param1
                """),
                {"param1": job_name}
            )

            return dict(result.fetchone())

        except Exception as e:
            raise Exception(f"Transaction failed: {e}")


def get_all_job_inst_tasks_as_list(job_inst_id: int, etl_step: str):
    """
    # Use case|| MULTIPLE SQL EXTRACTS FROM MULTIPLE TABLES ||
    # Return all job instance tasks as a list of dictionaries for task mapping.
    """
    engine = get_engine_for_metadata()

    with engine.connect() as connection:
        try:
            result = connection.execute(
                text("""
                    EXEC [metadata].[sp_get_job_inst_task] 
                        @p_job_inst_id = :param1,
                        @p_etl_step = :param2
                """),
                {"param1": job_inst_id, "param2": etl_step}
            )

            return [dict(row) for row in result]  # 👈 Fully materialize and return list

        except Exception as e:
            raise Exception(f"Transaction failed: {e}")

def get_all_job_inst_tasks(job_inst_id: int, etl_step: str):
    """
    # Use case|| STREAMING ||
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

            _info_msg =f"Starting Job: {job_name} || ETL steps: {etl_steps} || Full load:  {is_full_load} "
            print(_info_msg)
            log_info(job_inst_id, 'fetch_job_params'
                     , _info_msg
                     , context=f"{inspect.currentframe().f_code.co_name}", logging_seq = 2)
            return data
        else:
            log_error(
                job_inst_id=job_inst_id,
                task_name="fetch_job_params",
                error_message=f"No job parameters found for job_inst_id: {job_inst_id}",
                context=f"{inspect.currentframe().f_code.co_name}", logging_seq = 2
            )
            raise Exception(f"No job parameters found for job_inst_id: {job_inst_id}")

def parse_table_name(full_name):
    parts = full_name.split('.')
    if len(parts) == 3:
        db, schema, table = parts
    elif len(parts) == 2:
        schema, table = parts
    elif len(parts) == 1:
        schema = 'dbo'
        table = parts[0]
    else:
        raise ValueError("Unrecognized table format")
    return schema, table




