from datetime import datetime
import os
import sys
import inspect
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from typing import List, Dict
from airflow.utils.trigger_rule import TriggerRule
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../utils')))
from helper import (get_engine_for_metadata, log_error, log_info, log_job_task, complete_job
, get_all_job_inst_tasks, create_job_inst_and_log, get_job_inst_info, get_all_job_inst_tasks_as_list)
from process_task import JobTask
sys.path.append('/opt/airflow/utils')


@task
def start_job(job_id: int) -> int:
    """
        # call [metadata].[sp_crud_job_inst] stored proc to:
        # 1. creates job_inst_id and related tasks
        # 2. logs process start
    """
    return create_job_inst_and_log(job_id)

@task(trigger_rule=TriggerRule.ALL_DONE)
def finalize_job(job_data: dict):
    """
        Mark job completion
        Note: The @task(trigger_rule=TriggerRule.ALL_DONE) ensures it runs even if upstream ETL tasks are skipped
    """
    log_info(job_data['job_inst_id'], 'finalize_job'
             , "*** FINISHED", context=f"{inspect.currentframe().f_code.co_name}", task_status="succeeded")

    complete_job(job_inst_id=job_data["job_inst_id"], success=True)

@task
def fetch_job_params(job_inst_id: int) -> dict:
    """
        # call [metadata].[sp_crud_job_inst]  stored proc to:
        # get job parameters  from metadata tables
    """
    return get_job_inst_info(job_inst_id)

@task
def get_job_tasks(job_inst_id: int, etl_step: str) -> List[Dict]:
    """
    returns a list of dicts, one per job task row.
    by calling get_all_job_inst_tasks_as_list()
    """
    try:
        print(f"Retrieving tasks for job_inst_id: {job_inst_id} | step: {etl_step}")
        tasks = get_all_job_inst_tasks_as_list(job_inst_id, etl_step)
        log_info(job_inst_id, f"get_tasks_{etl_step}", f"Retrieved {len(tasks)} tasks", inspect.currentframe().f_code.co_name)
        return tasks
    except Exception as e:
        log_error(job_inst_id, f"get_tasks_{etl_step}", str(e), inspect.currentframe().f_code.co_name)
        raise

@task
def process_task(task_data: dict):
    """
    a single-task wrapper that takes one task_data dict and instantiates JobTask for processing.
    """
    try:
        JobTask(task_data).process()
    except Exception as e:
        log_error(task_data["job_inst_id"], task_data["job_task_name"], str(e), inspect.currentframe().f_code.co_name)
        raise
"""
    ETL-step checking functions, that are used 
    for conditional logic to skip ETL steps dynamically
    # AirflowSkipException needed for skipped branches to be marked as "skipped", not failed or errored.
"""
###########  ###########################

@task
def should_extract(job_data: Dict) -> int:
    try:
        if 'E' not in job_data['etl_steps'].upper():
            raise AirflowSkipException("Skipping Extract step")
        return job_data['job_inst_id']
    except Exception as e:
        log_error(job_data['job_inst_id'], "should_extract", str(e), inspect.currentframe().f_code.co_name)
        raise

@task
def should_transform(job_data: Dict) -> int:
    try:
        if 'T' not in job_data['etl_steps'].upper():
            raise AirflowSkipException("Skipping Transform step")
        return job_data['job_inst_id']
    except Exception as e:
        log_error(job_data['job_inst_id'], "should_transform", str(e), inspect.currentframe().f_code.co_name)
        raise

@task
def should_load(job_data: Dict) -> int:
    try:
        if 'L' not in job_data['etl_steps'].upper():
            raise AirflowSkipException("Skipping Load step")
        return job_data['job_inst_id']
    except Exception as e:
        log_error(job_data['job_inst_id'], "should_load", str(e), inspect.currentframe().f_code.co_name)
        raise

"""
    ETL task group - main functionality
    
    For each ETL step, each process_task[n] runs in parallel, one per job task row:
    etl_group
    ├── get_job_tasks (for E or T or L )
    └── process_task[0]
        └── process_task[1]
        └── process_task[2]
        
    # Notes:
    #.expand() works only on Airflow tasks with @task decorators — not arbitrary class methods.
    # should_*() tasks check for the corresponding step in etl_steps, and skip downstream tasks if the step isn't included.
    # process_task.expand(task_data=tasks) automatically fans out to parallel executions — one per item in tasks.
    # (it is guarded by those should_* triggers.)
    # .override(task_id=...) to make notations in the Airflow logs in each step.
"""
@task_group
def etl_group(job_data: Dict):
    extract_trigger = should_extract(job_data)
    extract_tasks = process_task.expand(
        task_data=get_job_tasks.override(task_id="get_extract_tasks")(extract_trigger, "E")
    )

    transform_trigger = should_transform(job_data)
    transform_tasks = process_task.expand(
        task_data=get_job_tasks.override(task_id="get_transform_tasks")(transform_trigger, "T")
    )

    load_trigger = should_load(job_data)
    load_tasks = process_task.expand(
        task_data=get_job_tasks.override(task_id="get_load_tasks")(load_trigger, "L")
    )


    ############# Dependencies #################################
    extract_tasks >> transform_tasks >> load_tasks
    ############################################################

# Step 3: Define the DAG
@dag(
    dag_id="ClientA_SQLServer_ETL_dag",
    schedule="0 9 * * *",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["etl", "mssql"],
)

def run_etl_dag():
    p_job_id = 1 # job_id for 'ClientA_SQLServer_ETL' job

    # DAG execution flow
    job_inst_id = start_job(p_job_id)        # Create job instance id
    job_data = fetch_job_params(job_inst_id) # Get params for this job instance id
    etl_tasks = etl_group(job_data)          # Group of extract, transform, and load tasks

    # finalize_job always run even when we skip ETL steps, because of  @task(trigger_rule=TriggerRule.ALL_DONE)
    job_done = finalize_job(job_data)        # Complete the job with success (runs after etl_group)

    etl_tasks >> job_done                    # Defining dependency: etl_group must finish before complete_job runs


dag = run_etl_dag()





