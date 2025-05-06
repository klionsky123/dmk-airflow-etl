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
from airflow import DAG
from airflow.decorators import task, task_group

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../utils')))
# From the module helper.py, inside the package utils, import these functions:
from helper import (get_engine_for_metadata, log_error, log_info, log_job_task,
                    complete_job, get_all_job_inst_tasks, create_job_inst_and_log, get_job_inst_info)
from process_task import (process_extract_csv_file,process_extract_task_mssql, process_api_data,process_transform_task_mssql
,process_load_task_mssql)
sys.path.append('/opt/airflow/utils')


@task()
def start_job(job_id: int)-> int:
    """
        # call [metadata].[sp_crud_job_inst] stored proc to:
        # 1. creates job_inst_id and related tasks
        # 2. logs process start
    """
    return create_job_inst_and_log(job_id)

@task()
def finalize_job(job_data:dict):
    # Mark job completion:
    log_info(job_data['job_inst_id'], 'finalize_job'
             , "*** FINISHED", context="finalize_job()", task_status="succeeded")

    complete_job(job_inst_id=job_data["job_inst_id"], success=True)

@task
def fetch_job_params(job_inst_id: int) -> dict:
    """
        # call [metadata].[sp_crud_job_inst]  stored proc to:
        # get job parameters  from metadata tables
    """
    return get_job_inst_info(job_inst_id)

# Step 2: Define the ETL task group
@task_group
def etl_group(job_data: dict):
    @task
    def extract(data):
        print(data)

        if 'E' not in data['etl_steps'].upper():
            print(f"Skipping 'E' step")
            log_info(job_inst_id=data['job_inst_id']
                     , task_name= 'etl_group'
                     , info_message=f"Skipping 'E' step"
                     , context="extract()"
                     )
            return

        job_inst_id = data['job_inst_id']
        step_name = "extract"
        etl_step = "E"

        try:
            engine = get_engine_for_metadata()

            # Get all tasks for the current job instance:
            # @p_etl_step ="E", for extract
            rows = get_all_job_inst_tasks(job_inst_id, etl_step)

            for row in rows:
                job_inst_task_id = row["job_inst_task_id"]
                job_task_name = row["job_task_name"]
                source_table = row["src_fully_qualified_tbl_name"]
                target_table = row["tgt_fully_qualified_tbl_name"]

                print(f"Started {job_task_name}  task || {source_table} --> {target_table}")
                log_info(job_inst_id=job_inst_id
                         , task_name=job_task_name
                         , info_message=f"Started {job_task_name}  task ||  {source_table} --> {target_table}"
                         , context="extract()")

                conn_type = row["conn_type"].lower()

                try:
                    if  conn_type == "db":
                        db_type = row["db_type"].lower()
                        if "mssql" in db_type:
                            row_count = process_extract_task_mssql(row)

                    # REST API + Headers + Payload:
                    elif "api" in conn_type:
                       process_api_data(row)

                    elif conn_type == "file":
                        row_count=process_extract_csv_file(row)
                        # file_path = row["file_path"]
                        # df = pd.read_csv(file_path)
                        # print(f"[{job_inst_id}] CSV loaded: {df.shape[0]} rows, {df.shape[1]} columns")

                    elif conn_type == "parquet":
                        file_path = row["file_path"]
                        df = pd.read_parquet(file_path)
                        print(f"[{job_inst_id}] Parquet loaded: {df.shape[0]} rows, {df.shape[1]} columns")

                    else:
                        print(f"[{job_inst_id}] Unknown connection_type: {conn_type}")
                except Exception as e:
                    print(f"[{job_inst_id}] Extract task [{row.get('job_task_name')}] failed: {e}")
                    log_error(job_inst_id, step_name, str(e), "extract()")  # [metadata].[log_dtl] table
                    complete_job(job_inst_id, success=False)                # [metadata].[log_header] table
                    raise

                # report success:
                log_job_task(job_inst_task_id, "succeeded")  # [metadata].[job_inst_task] table

                print(f"{job_task_name} task succeeded")
                log_info(job_inst_id=job_inst_id
                         , task_name='extract'
                         , info_message=f"Finished {job_task_name} task with success"
                         , context="extract()"
                         )

        except Exception as e:
            log_error(data['job_inst_id'], "extract", str(e), "extract()")      # [metadata].[log_dtl] table
            complete_job(data['job_inst_id'], success=False)                    # [metadata].[log_header] table
            raise

    @task
    def transform(data):

        job_inst_id = data['job_inst_id']
        etl_step = "T" # for Transform

        # exit if ETL steps don't include T (for transform)
        if 'T' not in data['etl_steps'].upper():
            print(f"Skipping 'T' step")
            log_info(job_inst_id=job_inst_id
                     , task_name= 'etl_group'
                     , info_message=f"Skipping 'T' step"
                     , context="transform()"
                     )
            return




        try:

            # Get all tasks for the current job instance:
            # @p_etl_step ="T", for extract
            rows = get_all_job_inst_tasks(job_inst_id, etl_step)

            for row in rows:
                job_inst_task_id = row["job_inst_task_id"]
                job_task_name = row["job_task_name"]
                source_table = row["src_fully_qualified_tbl_name"]
                target_table = row["tgt_fully_qualified_tbl_name"]

                print(f"Processing 'T' step || {source_table} --> {target_table}")
                log_info(job_inst_id=job_inst_id
                         , task_name=job_task_name
                         , info_message=f"Processing || {source_table} --> {target_table}"
                         , context="transform()"
                         )


                try:
                        process_transform_task_mssql(
                            job_inst_id=job_inst_id,
                            job_inst_task_id=job_inst_task_id,
                            conn_str=row["conn_str"],
                            sql_text=row["sql_text"]
                        )
                except Exception as e:
                        print(f"[{job_inst_id}] transform task [{job_task_name}] failed: {e}")
                        log_error(job_inst_id, "transform", str(e), "transform()")  # [metadata].[log_dtl] table
                        complete_job(job_inst_id, success=False)  # [metadata].[log_header] table
                        raise
                #report success:
                log_job_task(job_inst_task_id, "succeeded")  # [metadata].[job_inst_task] table
        except Exception as e:
            log_error(job_inst_id, "transform", str(e), "transform()")  # [metadata].[log_dtl] table
            complete_job(job_inst_id, success=False)  # [metadata].[log_header] table
            raise


    @task
    def load(data):
        job_inst_id = data['job_inst_id']
        etl_step = "L" # for Load

        if 'L' not in data['etl_steps'].upper():
            print(f"Skipping 'L' step")
            log_info(job_inst_id=job_inst_id
                     , task_name= 'etl_group'
                     , info_message=f"Skipping 'L' step"
                     , context="load()"
                     )
            return
        try:

            # Get all tasks for the current job instance:
            # @p_etl_step ="L", for load
            rows = get_all_job_inst_tasks(job_inst_id, etl_step)

            for row in rows:
                job_inst_task_id = row["job_inst_task_id"]
                job_task_name = row["job_task_name"]
                source_table = row["src_fully_qualified_tbl_name"]
                target_table = row["tgt_fully_qualified_tbl_name"]

                print(f"Processing 'L' step || {source_table} --> {target_table}")
                log_info(job_inst_id=job_inst_id
                         , task_name=job_task_name
                         , info_message=f"Processing || {source_table} --> {target_table}"
                         , context="load()"
                         )


                try:
                        process_load_task_mssql(
                            job_inst_id=job_inst_id,
                            job_inst_task_id=job_inst_task_id,
                            conn_str=row["conn_str"],
                            sql_text=row["sql_text"]
                        )
                except Exception as e:
                        print(f"[{job_inst_id}] load task [{job_task_name}] failed: {e}")
                        log_error(job_inst_id, "load", str(e), "load()")  # [metadata].[log_dtl] table
                        complete_job(job_inst_id, success=False)  # [metadata].[log_header] table
                        raise
                #report success:
                log_job_task(job_inst_task_id, "succeeded")  # [metadata].[job_inst_task] table
        except Exception as e:
            log_error(job_inst_id, "load", str(e), "load()")  # [metadata].[log_dtl] table
            complete_job(job_inst_id, success=False)  # [metadata].[log_header] table
            raise

    # Define tasks
    extracted = extract(p_job_data)
    transformed = transform(p_job_data)
    loaded = load(p_job_data)

    # Define dependencies
    extracted >> transformed >> loaded

# Step 3: Define the DAG
with DAG(
    dag_id="ClientB_FLAT_FILE_ETL_dag",
    schedule="0 9 * * *",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["etl", "sql", "api", "files", "parquet", "csv"],
) as dag:
    # First task: Fetch job parameters
    p_job_id = 3  # 'ClientB_FLAT_FILE_ETL' job_id
    p_job_inst_id = start_job(p_job_id)
    p_job_data = fetch_job_params(p_job_inst_id)

    # Execute the ETL task group
    etl_tasks = etl_group(p_job_data)  # Group of extract, transform, and load tasks
    
    # Complete the job with success (runs after etl_group)
    done = finalize_job(p_job_data)
    
    # Define dependency: etl_group must finish before complete_job runs
    etl_tasks >> done
    
    

