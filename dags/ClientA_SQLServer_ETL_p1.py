from datetime import datetime
import os
import sys
import inspect
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import List, Dict
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../utils')))
from helper import (get_engine_for_metadata, log_error, log_info, log_job_task, complete_job, get_job_config
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
             , "*** FINISHED", context=f"{inspect.currentframe().f_code.co_name}", task_status="succeeded", logging_seq = 1000)

    complete_job(job_inst_id=job_data["job_inst_id"], success=True)

@task
def fetch_job_params(job_inst_id: int) -> dict:
    """
        # call [metadata].[sp_crud_job_inst]  stored proc to:
        # get job parameters  from metadata tables
    """
    return get_job_inst_info(job_inst_id)

@task
def get_job_tasks(job_data: dict, etl_step: str) -> List[Dict]:
    """
    returns a list of dicts, one per job task row.
    by calling get_all_job_inst_tasks_as_list()
    """
    logging_seq = 10

    if etl_step.upper() == "E":
        logging_seq = logging_seq + 100
    elif etl_step.upper() == "T":
        logging_seq = logging_seq + 200
    elif etl_step.upper() == "L":
        logging_seq = logging_seq + 300


    job_inst_id = job_data['job_inst_id']
    try:
        print(f"Retrieving tasks for job_inst_id: {job_inst_id} | step: {etl_step}")
        task_list = get_all_job_inst_tasks_as_list(job_inst_id, etl_step)
        log_info(job_inst_id,
                 f"get_tasks_{etl_step}",
                 f"Retrieved {len(task_list)} tasks || | step: {etl_step}",
                 inspect.currentframe().f_code.co_name, "running", logging_seq)
        for idx, _task in enumerate(task_list):
            _task["sequence_num"] = (idx + 1) * 20  # to use as logging_seq
            _task["task_thread_num"] = (idx + 1)  # to display in the log
        return task_list
    except Exception as e:
        log_error(job_inst_id, f"get_tasks_{etl_step}", str(e), inspect.currentframe().f_code.co_name, logging_seq)
        raise


@task(max_active_tis_per_dagrun=5) # limit the number of concurrent mapped job_tasks to 5 (to avoid server overload/API Rate limit etc.)
def process_task(task_data: dict):
    """
    a single-task wrapper that takes one task_data dict and instantiates JobTask for processing.
    """
    try:
        JobTask(task_data).process()
    except Exception as e:
        log_error(task_data["job_inst_id"], task_data["job_task_name"], str(e), inspect.currentframe().f_code.co_name)
        raise

# -- Utility log wrapper @task
@task
def log_phase_start(job_data: dict, phase: str, logging_seq: int):
    job_inst_id = job_data['job_inst_id']
    log_info(
        job_inst_id=job_inst_id,
        task_name=phase,
        info_message=f"Started {phase.upper()} phase",
        context="etl_group",
        task_status="running",
        logging_seq=logging_seq
    )
    return job_data
"""
    ETL-step checking functions, that are used 
    for conditional logic to skip ETL steps dynamically
    # AirflowSkipException needed for skipped branches to be marked as "skipped", not failed or errored.
"""
###########  ###########################

# -- Trigger checkers wrapped with @task to allow runtime execution
@task
def should_extract(job_data: dict):
    if 'E' not in job_data['etl_steps'].upper():
        log_info(
            job_inst_id=job_data['job_inst_id'],
            task_name='extract',
            info_message="Skipping Extract phase",
            context="should_extract()",
            task_status="skipped",
            logging_seq=101
        )
        raise AirflowSkipException("Skipping Extract step")
    return job_data

@task
def should_transform(job_data: dict):
    if 'T' not in job_data['etl_steps'].upper():
        log_info(
            job_inst_id=job_data['job_inst_id'],
            task_name='transform',
            info_message="Skipping Transform phase",
            context="should_transform()",
            task_status="skipped",
            logging_seq=201
        )
        raise AirflowSkipException("Skipping Transform step")
    return job_data

@task
def should_load(job_data: dict):
    if 'L' not in job_data['etl_steps'].upper():
        log_info(
            job_inst_id=job_data['job_inst_id'],
            task_name='load',
            info_message="Skipping Load phase",
            context="should_load()",
            task_status="skipped",
            logging_seq=301
        )
        raise AirflowSkipException("Skipping Load step")
    return job_data

@task_group(group_id="extract_group")
def extract_group(job_data: Dict):
    extract_log = log_phase_start.override(task_id="log_phase_start__E")(job_data, "extract", logging_seq=100)
    extract_trigger = should_extract.override(task_id="should_extract__E")(job_data)
    extract_log >> extract_trigger
    extract_tasks = process_task.override(task_id="process_task__E").expand(
        task_data=get_job_tasks.override(task_id="get_extract_tasks")(extract_trigger, "E")
    )
    return extract_tasks


@task_group(group_id="transform_group")
def transform_group(job_data: Dict):
    transform_log = log_phase_start.override(task_id="log_phase_start__T")(job_data, "transform", logging_seq=200)
    transform_trigger = should_transform.override(task_id="should_transform__T")(job_data)
    transform_log >> transform_trigger
    transform_tasks = process_task.override(task_id="process_task__T").expand(
        task_data=get_job_tasks.override(task_id="get_transform_tasks")(transform_trigger, "T")
    )
    return transform_tasks


@task_group(group_id="load_group")
def load_group(job_data: Dict):
    load_log = log_phase_start.override(task_id="log_phase_start__L")(job_data, "load", logging_seq=300)
    load_trigger = should_load.override(task_id="should_load__L")(job_data)
    load_log >> load_trigger
    load_tasks = process_task.override(task_id="process_task__L").expand(
        task_data=get_job_tasks.override(task_id="get_load_tasks")(load_trigger, "L")
    )
    return load_tasks

@task_group(group_id="etl_group")
def etl_group(job_data: Dict):
    extract_tasks = extract_group(job_data)
    transform_tasks = transform_group(job_data)
    load_tasks = load_group(job_data)

    # Set dependencies between groups
    extract_tasks >> transform_tasks >> load_tasks


p_job_name = "ClientA_SQLServer_E_part1"
job_config = get_job_config(p_job_name)
p_job_id = job_config.get("job_id")
p_tags =[tag.strip() for tag in job_config.get("tags").split(",")] # converting comma separated string to a list of strings
send_notification = job_config.get("send_notification", False)  # boolean flag

@dag(
    dag_id= "ClientA_SQLServer_E_part1",
    schedule="0 9 * * *",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=p_tags,
    params={"p_job_id": p_job_id, "tags": p_tags},
)

def run_etl_dag():
        # DAG execution flow
        job_inst_id = start_job(p_job_id)        # Create job instance id
        job_data = fetch_job_params(job_inst_id) # Get params for this job instance id
        etl_tasks = etl_group(job_data)          # Group of extract, transform, and load tasks

        # finalize_job always run even when we skip ETL steps, because of  @task(trigger_rule=TriggerRule.ALL_DONE)
        job_done = finalize_job(job_data)        # Complete the job with success (runs after etl_group)

        etl_tasks >> job_done                    # Defining dependency: etl_group must finish before complete_job runs

        # Trigger another DAG after ETL completion
        trigger_etl_job = TriggerDagRunOperator(
            task_id="trigger_etl_job",
            trigger_dag_id="ClientA_SQLServer_ETL_part2",  # The next ETL DAG to trigger
            wait_for_completion=True
        )

        job_done >> trigger_etl_job  # Ensure it runs after completion

    # Create email tasks if notifications are enabled
        if send_notification:

            send_success_email = EmailOperator(
                task_id="send_success_email",
                to="xxx@klionsky.org",
                subject="✅ ETL Success: {{ dag.dag_id }}",
                html_content="""
                    <h3>ETL Job Completed Successfully</h3>
                    <p><b>DAG:</b> {{ dag.dag_id }}</p>
                    <p><b>Execution Time:</b> {{ ts }}</p>
                """,
            )

            @task(trigger_rule=TriggerRule.ONE_FAILED)
            def send_failure_email():
                send_email(
                    to="xxx@klionsky.org",
                    subject="❌ ETL Failed: {{ dag.dag_id }}",
                    html_content="<p>One or more tasks in the DAG failed. Check logs.</p>",
                )

            job_done >> send_success_email
            etl_tasks >> send_failure_email()

dag = run_etl_dag()





