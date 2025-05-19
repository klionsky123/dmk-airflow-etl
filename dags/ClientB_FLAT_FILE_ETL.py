from datetime import datetime
import os
import sys
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../utils')))
from helper import (get_engine_for_metadata, log_error, log_info, log_job_task, complete_job
, get_all_job_inst_tasks, create_job_inst_and_log, get_job_inst_info, get_job_config)
from process_task import JobTask

sys.path.append('/opt/airflow/utils')


@task()
def start_job(job_id: int) -> int:
    """
        # call [metadata].[sp_crud_job_inst] stored proc to:
        # 1. creates job_inst_id and related tasks
        # 2. logs process start
    """
    return create_job_inst_and_log(job_id)


@task()
def finalize_job(job_data: dict):
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
    def extract(job_data):
        print(job_data)

        if 'E' not in job_data['etl_steps'].upper():
            print(f"Skipping 'E' step")
            log_info(job_inst_id=job_data['job_inst_id'],
                     task_name='etl_group',
                     info_message="Skipping 'E' step",
                     context="extract()")
            return

        job_inst_id = job_data['job_inst_id']
        etl_step = "E"  # for Extract

        try:
            # 1. Get all tasks for the current job instance
            # 2. Process them in a loop one task at a time
            [JobTask(_task).process() for _task in get_all_job_inst_tasks(job_inst_id, etl_step)]

        except Exception as e:
            log_error(job_inst_id, "extract", str(e), "extract()")
            complete_job(job_inst_id, success=False)
            raise

    @task
    def transform(data):

        job_inst_id = data['job_inst_id']
        etl_step = "T"  # for Transform

        # exit if ETL steps don't include T (for transform)
        if 'T' not in data['etl_steps'].upper():
            print(f"Skipping 'T' step")
            log_info(job_inst_id=job_inst_id
                     , task_name='etl_group'
                     , info_message=f"Skipping 'T' step"
                     , context="transform()"
                     )
            return

        try:
            # 1. Get all tasks for the current job instance
            # 2. Process them in a loop one task at a time
            [JobTask(_task).process() for _task in get_all_job_inst_tasks(job_inst_id, etl_step)]

        except Exception as e:
            log_error(job_inst_id, "transform", str(e), "transform()")  # [metadata].[log_dtl] table
            complete_job(job_inst_id, success=False)  # [metadata].[log_header] table
            raise

    @task
    def load(data):
        job_inst_id = data['job_inst_id']
        etl_step = "L"  # for Load

        if 'L' not in data['etl_steps'].upper():
            print(f"Skipping 'L' step")
            log_info(job_inst_id=job_inst_id
                     , task_name='etl_group'
                     , info_message=f"Skipping 'L' step"
                     , context="load()"
                     )
            return
        try:
            # 1. Get all tasks for the current job instance
            # 2. Process them in a loop one task at a time
            [JobTask(_task).process() for _task in get_all_job_inst_tasks(job_inst_id, etl_step)]
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


#####################################################
# Step 3: Define the DAG
with DAG(
        dag_id="ClientB_FLAT_FILE_ETL_dag",
        schedule="0 9 * * *",
        start_date=datetime(2023, 4, 1),
        catchup=False,
        tags=["etl", "csv"],
) as dag:
    # get job_id by job name:
    job_config = get_job_config("ClientB_FLAT_FILE_ETL")
    p_job_id = job_config.get("job_id")
    send_notification = job_config.get("send_notification", False)  # boolean flag

    # set job instance id:
    p_job_inst_id = start_job(p_job_id)
    p_job_data = fetch_job_params(p_job_inst_id)

    # Execute the ETL task group
    etl_tasks = etl_group(p_job_data)  # Group of extract, transform, and load tasks

    # Complete the job with success (runs after etl_group)
    done = finalize_job(p_job_data)

    # Define dependency: etl_group must finish before complete_job runs
    etl_tasks >> done

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

        done >> send_success_email
        etl_tasks >> send_failure_email()

