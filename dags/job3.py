from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import sys
import pandas as pd
import requests
import json
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from airflow import DAG
from airflow.decorators import task, task_group

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../utils')))
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from helper import get_engine_for_metadata, write_to_log,log_error, log_info, log_job_task,complete_job
# from airflow_docker.utils.helper import get_engine_for_metadata, write_to_log,log_error, log_info, log_job_task,complete_job
sys.path.append('/opt/airflow/utils')
# from helper import get_engine_for_metadata, write_to_log,log_error, log_info, log_job_task,complete_job

def process_extract_task_mssql(
    job_inst_id: int,
    job_inst_task_id: int,
    job_task_name: str,
    task_status: str,
    conn_str: str,
    sql_text: str,
    target_table: str,
    is_large: bool,
    del_temp_data: bool 
) -> int:
    """
    Purpose:
        Process 'extract' step of a job instance task.
        - For large datasets: export to CSV and bulk insert
        - For small datasets: load into temp table then copy to target

    Called from: extract()
    """
    log_job_task(job_inst_task_id, "running")   # [metadata].[job_inst_task] table
    
    engine_tgt = get_engine_for_metadata()  # Target
    engine_src = create_engine(conn_str)    # Source
    
    log_info(job_inst_id = job_inst_id
        , task_name = job_task_name
        , info_message = f"source || {engine_src}"
        , context="process_extract_task_mssql()"
        ) 
    log_info(job_inst_id = job_inst_id
    , task_name = job_task_name
    , info_message = f"target || {engine_tgt}"
    , context="process_extract_task_mssql()"
    )
    print(f"source || {engine_src}")
    print(f"target || {engine_tgt}")

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    try:
        with engine_src.connect() as conn_src:
            df = pd.read_sql(text(sql_text), con=conn_src)
            print(sql_text)
            if is_large:
                # Write to CSV
                # file_path = f"//192.168.86.96/shared/bulk_files/{target_table.replace('.', '_')}_{timestamp}.csv"
                # df.to_csv(file_path, index=False)
                
                filename = f"{target_table.replace('.', '_')}_{timestamp}.csv"

                # This is inside the container, but it's mapped to the Windows share
                docker_shared_path = "/mnt/sql_shared/bulk_files/"
                file_path = os.path.join(docker_shared_path, filename)

                # Write CSV to shared folder
                df.to_csv(file_path, index=False)

                # The path as seen by SQL Server (UNC path)
                sql_server_path = f"\\\\192.168.86.96\\shared\\bulk_files\\{filename}"
                 
                # Append a query to capture the row count after the bulk insert
                bulk_insert_sql_with_rowcount = f"""
                    BULK INSERT {target_table}
                    FROM '{sql_server_path}'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\\n',
                        TABLOCK
                    );

                    SELECT @@ROWCOUNT AS inserted_rows;
                """

                with engine_tgt.connect() as conn_tgt:
                    result = conn_tgt.execution_options(autocommit=True).execute(text(bulk_insert_sql_with_rowcount))

                    # Fetch the row count from the result
                    row_count = result.fetchone()["inserted_rows"]
                    print(f"Number of rows inserted: {row_count}")
                    log_info(job_inst_id = job_inst_id
                        , task_name = job_task_name
                        , info_message = f"Target table [{target_table}] || inserted {row_count} rows"
                        , context="process_extract_task_mssql()"
                        )


                # Delete temp file if needed
                if del_temp_data and os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Dropped {file_path}")
                    log_info(job_inst_id = job_inst_id
                        , task_name = job_task_name
                        , info_message = f"Dropped {file_path}"
                        , context="process_extract_task_mssql()"
                        )

            else:           
                
                # Small dataset: temp table flow
                # staging the data in a temporary table first, a common pattern in ETL for validation
                
                # just getting the last part, which is the actual table name
                temp_table = f"temp_{target_table.split('.')[-1]}_{job_inst_id}"
                print (f"Temp table: {temp_table}")

                with engine_tgt.connect() as conn_tgt:
                    # Create empty temp table
                    # df.head(0) gives the column structure without rows.
                    # if_exists="replace" drops and recreates the table if it already exists.
                    df.head(0).to_sql(temp_table, con=conn_tgt, if_exists="replace", index=False)
                    
                    # Insert data
                    df.to_sql(temp_table, con=conn_tgt, if_exists="append", index=False, method="multi")
                    

                    # Move to target table
                    # insert_sql = f"truncate table {target_table}; INSERT INTO {target_table} SELECT * FROM {temp_table}"
                    insert_sql = f"INSERT INTO {target_table} SELECT * FROM {temp_table}"
                    result = conn_tgt.execute(text(insert_sql))
                    rowcount = result.rowcount
                    print(f"Number of rows inserted: {rowcount}")
                    log_info(job_inst_id = job_inst_id
                        , task_name = job_task_name
                        , info_message = f"Target table [{target_table}] || inserted {rowcount} rows"
                        , context="process_extract_task_mssql()"
                        )

                    # Drop temp table if requested
                    if del_temp_data:
                        try:
                            conn_tgt.execute(text(f"DROP TABLE {temp_table}"))
                            print(f"Dropped temp table {temp_table}")
                            log_info(job_inst_id = job_inst_id
                                , task_name = job_task_name
                                , info_message = f"Dropped temp table || {temp_table}"
                                , context="process_extract_task_mssql()"
                                )
                        except Exception as drop_err:
                            print(f"Warning: Failed to drop temp table {temp_table}: {drop_err}")
                            log_error(job_inst_id = job_inst_id
                                    , task_name = job_task_name
                                    , error_message = f"Failed to drop temp table {temp_table}: {drop_err}"
                                    , context="process_extract_task_mssql()"
                                    )                        
                            log_job_task(job_inst_task_id, "failed")    # [metadata].[job_inst_task] table
                            
            log_job_task(job_inst_task_id, "succeeded") # [metadata].[job_inst_task] table
        
            return len(df)

    except Exception as e:
        # [metadata].[log_dtl] table:
        log_error(job_inst_id = job_inst_id
                , task_name = job_task_name
                , error_message = str(e)
                , context="process_extract_task_mssql()"
                ) 
        log_job_task(job_inst_task_id, "failed")    # [metadata].[job_inst_task] table
        raise


@task()
def start_job(job_id: int):
    """
        # call [metadata].[sp_crud_job_inst] stored proc to:
        # 1. create job_inst_id and related tasks
        # 2. log process start  
    """
    engine = get_engine_for_metadata()

    with engine.connect() as connection:
        # Begin a transaction
        trans = connection.begin()
        try:
            # Execute the stored procedure
            result = connection.execute(
                text("""
                    EXEC [metadata].[sp_crud_job_inst] 
                        @p_action = :param1,
                        @p_job_id = :param2
                """),
                {"param1": "INS", "param2": job_id}
            ).fetchone()

            # Commit the transaction
            trans.commit()

            if result:
                job_inst_id = result[0]  # this returns a single integer (job_inst_id)
                print(f"[{job_inst_id}] Status set to running")
                return job_inst_id
            else:
                raise Exception("Failed to create or retrieve job_inst_id from [metadata].[sp_crud_job_inst] stored procedure.")
        except Exception as e:
            # Rollback the transaction in case of an error
            trans.rollback()
            raise Exception(f"Transaction failed: {e}")

@task()
def finalize_job(job_data):
    try:
        complete_job(job_inst_id=job_data["job_inst_id"], success=True)
    except Exception as e:
        print(f"[{job_data['job_inst_id']}] Finalization failed: {e}")
        complete_job(job_inst_id=job_data["job_inst_id"], success=False)
        
@task
def fetch_job_params(job_inst_id: int) -> dict:
    # returns a single row:
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
            return dict(record)
        else:
            log_error(
                job_inst_id=job_inst_id,
                task_name="fetch_job_params",
                error_message=f"No job parameters found for job_inst_id: {job_inst_id}",
                context="fetch_job_params()"
            )
            raise Exception(f"No job parameters found for job_inst_id: {job_inst_id}")

def fetch_api_key(job_inst_id, job_inst_task_id, job_task_name, auth_url, conn_str_name, username, password):

    log_job_task(job_inst_task_id, "running")  # [metadata].[job_inst_task] table
    if conn_str_name == "conn_ReqRes_api_key":
        payload = {
            "email": username,
            "password": password
        }
    else:
        payload = {
            "username": username,
            "password": password
        }

    try:
        response = requests.post(auth_url, json=payload)
        response.raise_for_status()
        api_key = response.json().get("token")
        if not api_key:
            log_error(job_inst_id=job_inst_id
                      , task_name=job_task_name
                      , error_message=f"API key not found in response"
                      , context="fetch_api_key()"
                      )
            log_job_task(job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
            raise ValueError("API key not found in response")
        print(f"API key received || {api_key}")
        log_info(job_inst_id=job_inst_id
                 , task_name=job_task_name
                 , info_message=f"API key received || {api_key}"
                 , context="process_api_data()"
                 )
        return api_key
    except Exception as e:
        print(f"Failed to fetch API key || {e}")
        log_error(job_inst_id=job_inst_id
                  , task_name=job_task_name
                  , error_message=f"Failed to fetch API key || {e}"
                  , context="fetch_api_key()"
                  )
        log_job_task(job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
        raise


def process_api_data(job_inst_id, job_inst_task_id, job_task_name,
                     api_key, request_url, request_method, payload_json, target_table, sql_text):

    """
    1. save into the table
    2. additionally, save into file for later reprocessing when needed
    """

    log_job_task(job_inst_task_id, "running")  # [metadata].[job_inst_task] table
    payload = json.loads(payload_json or "{}")
    headers = {"Authorization": f"Bearer {api_key}"}

    try:

        print(f"Request url || {request_url} || Headers || {headers}")
        log_info(job_inst_id=job_inst_id
                 , task_name=job_task_name
                 , info_message=f"Request url || {request_url} || Headers || {headers}"
                 , context="process_api_data()"
                 )


        response = requests.get(request_url, headers=headers)
        response.raise_for_status()
        json_payload = json.dumps(response.json())

        # 1. save into the table:
        engine = get_engine_for_metadata()  # Target
        with engine.connect() as connection:
            trans = connection.begin()
            try:

                proc_name = sql_text

                sql_text = f"""
                    EXEC {proc_name}  
                        @p_job_inst_id = :param1,
                        @p_json = :param2
                """

                connection.execute(
                    text(sql_text),
                    {"param1": job_inst_id, "param2": json_payload}
                )

                trans.commit()

            except Exception as e:
                # Rollback the transaction in case of an error
                trans.rollback()
                raise Exception(f"Transaction failed: {e}")

        # 2. additionally, save into file for reprocessing if needed:
        timestamp = datetime.utcnow().strftime("%Y_%m_%d")
        file_path = f"/opt/airflow/tmp/api_responses/{target_table.split('.')[-1]}_{job_inst_id}_{timestamp}.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as f:
            json.dump({
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "payload": payload,
                "response_text": response.text
            }, f, indent=2)

        print(f"[{job_inst_id}] API response saved to: {file_path}")
        log_info(job_inst_id=job_inst_id
                 , task_name=job_task_name
                 , info_message=f"[Job_inst_id || {job_inst_id} || API response saved to || {file_path}"
                 , context="process_api_data()"
                 )

        log_job_task(job_inst_task_id, "succeeded")  # [metadata].[job_inst_task] table


    except Exception as e:
        print(f"[{job_inst_id}] API processing failed || {e}")
        log_error(job_inst_id=job_inst_id
                  , task_name=job_task_name
                  , error_message=f"[{job_inst_id}] API processing failed: {e}"
                  , context="process_api_data()"
                  )
        log_job_task(job_inst_task_id, "failed")  # [metadata].[job_inst_task] table

        raise


# Step 2: Define the ETL task group
@task_group
def etl_group(job_data: dict):
    @task
    def extract(data):
        print(data)
        try:
            engine = get_engine_for_metadata()
            job_inst_id = data['job_inst_id']
            del_temp_data =data['del_temp_data']
            step_name ="extract"
            
            total_row_count = 0
            # Get all tasks for the current job instance:
            # @p_etl_step ="E", for extract
            with engine.connect() as connection:
                result = connection.execute(
                    text("""
                        EXEC [metadata].[sp_get_job_inst_task] 
                            @p_job_inst_id = :param1,
                            @p_etl_step = :param2
                    """),
                    {"param1": job_inst_id, "param2": "E"}
                )

                rows = result.fetchall()
                rows = [dict(row) for row in rows]  # 👈 Make rows accessible by column name

                for row in rows:
                    job_inst_task_id = row["job_inst_task_id"],
                    job_task_name = row["job_task_name"]
                    source_table = row["src_fully_qualified_tbl_name"],
                    target_table = row["tgt_fully_qualified_tbl_name"],

                    print(f"Processing || {source_table} --> {target_table}")
                    log_info(job_inst_id=job_inst_id
                             , task_name=job_task_name
                             , info_message=f"Processing || {source_table} --> {target_table}"
                             , context="extract()"
                             )

                    conn_type = row["conn_type"].lower()
                    try:
                        if "mssql" in conn_type.lower():
                            row_count = process_extract_task_mssql(
                                job_inst_id=job_inst_id,
                                job_inst_task_id=row["job_inst_task_id"],
                                job_task_name=row["job_task_name"],
                                task_status=row["task_status"],
                                conn_str=row["conn_str"],
                                sql_text=row["sql_text"],
                                target_table=row["tgt_fully_qualified_tbl_name"],
                                is_large=row.get("src_data_size", "small") == "large",
                                del_temp_data=del_temp_data
                            )
                            total_row_count += row_count

                        # REST API + Headers + Payload:
                        elif "api" in conn_type.lower():
                            # [two-step API call + data ingestion]
                            # 1. get api_key (if needed ),
                            # 2. then use it in the next step to get actual data
                            if row["need_token"] == 1:
                                api_key = fetch_api_key(
                                    job_inst_id=job_inst_id,
                                    job_inst_task_id=row["job_inst_task_id"],
                                    job_task_name=row["job_task_name"],
                                    auth_url=row["api_key_url"],
                                    conn_str_name=row["conn_str_name"],
                                    username = row["api_key_username"],
                                    password = row["api_key_pass"])

                            # use api_key from the above:
                                process_api_data(
                                    job_inst_id=job_inst_id,
                                    job_inst_task_id=row["job_inst_task_id"],
                                    job_task_name=row["job_task_name"],
                                    api_key=api_key,
                                    request_url=row["conn_str"],
                                    request_method=row["sql_type"],
                                    payload_json=row.get("payload_json"),
                                    target_table=row["tgt_fully_qualified_tbl_name"],
                                    sql_text=row["sql_text"]
                            )

                        elif conn_type == "csv":
                            file_path = row["file_path"]
                            df = pd.read_csv(file_path)
                            print(f"[{job_inst_id}] CSV loaded: {df.shape[0]} rows, {df.shape[1]} columns")

                        elif conn_type == "parquet":
                            file_path = row["file_path"]
                            df = pd.read_parquet(file_path)
                            print(f"[{job_inst_id}] Parquet loaded: {df.shape[0]} rows, {df.shape[1]} columns")

                        else:
                            print(f"[{job_inst_id}] Unknown connection_type: {conn_type}")
                    except Exception as e:
                        print(f"[{job_inst_id}] Extract task [{row.get('job_task_name')}] failed: {e}")
                        log_error(job_inst_id, step_name, str(e), "extract()")  # [metadata].[log_dtl] table
                        complete_job(job_inst_id, success=False)                               # [metadata].[log_header] table
                        raise
        except Exception as e:
            log_error(data['job_inst_id'], "extract", str(e), "extract()")      # [metadata].[log_dtl] table
            complete_job(data['job_inst_id'], success=False)                                    # [metadata].[log_header] table
            raise

    @task
    def transform(data):
        return f"Transformed data for job_id: {data['job_id']}"

    @task
    def load(data):
        print(f"Loaded data for job_id: {data['job_id']}")

    # Define tasks
    extracted = extract(job_data)
    transformed = transform(job_data)
    loaded = load(job_data)

    # Define dependencies
    extracted >> transformed >> loaded

# Step 3: Define the DAG
with DAG(
    dag_id="my_etl_dag",
    schedule="0 9 * * *",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["etl", "sql", "api", "files", "parquet", "csv"],
) as dag:
    # First task: Fetch job parameters
    job_id = 1  # Example job_id
    job_inst_id = start_job(job_id)  
    job_data = fetch_job_params(job_inst_id)

    # Execute the ETL task group
    etl_tasks = etl_group(job_data)  # Group of extract, transform, and load tasks
    
    # Complete the job with success (runs after etl_group)
    done = finalize_job(job_data)
    
    # Define dependency: etl_group must finish before complete_job runs
    etl_tasks >> done
    
    

