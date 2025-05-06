import requests
import json
from datetime import datetime
import os
from tenacity import retry, stop_after_attempt, wait_fixed
from helper import *

"""
1. save into the table
2. additionally, save into file for later reprocessing when needed
"""
# === Retry Decorator for API calls === #
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def reqres_fetch_and_save(row: dict):
    # [two-step API call + data ingestion]
    # 1. get api_key
    # 2. then use it in the next step to get actual data

    job_inst_id = row["job_inst_id"]
    job_inst_task_id = row["job_inst_task_id"]
    job_task_name = row["job_task_name"]

    log_job_task(job_inst_task_id, "running")  # [metadata].[job_inst_task] table
    etl_step = "E"
    row = get_job_inst_task_info(job_inst_id, etl_step, job_inst_task_id)

    request_url = row["conn_str"]
    payload_json = row.get("payload_json")
    target_table = row["tgt_fully_qualified_tbl_name"]
    sql_text = row["sql_text"]
    output_file_path = row["output_file_path"]
    payload = json.loads(payload_json or "{}") # request parameters


    try:
        # 1. get api key:
        api_key = fetch_api_key(
            job_inst_id=job_inst_id,
            job_inst_task_id=row["job_inst_task_id"],
            job_task_name=row["job_task_name"],
            auth_url=row["api_key_url"],
            conn_str_name=row["conn_str_name"],
            username=row["api_key_username"],
            password=row["api_key_pass"])

        headers = {"Authorization": f"Bearer {api_key}"}
        print(f"Request url || {request_url} || Headers || {headers}")
        log_info(job_inst_id=job_inst_id
                 , task_name=job_task_name
                 , info_message=f"Request url || {request_url} || Headers || {headers}"
                 , context="process_api_data()"
                 )

        response = requests.get(request_url, headers=headers)
        response.raise_for_status()
        json_payload = json.dumps(response.json())

        # 2. save into the table:
        load_all_into_db(job_inst_id, job_inst_task_id, job_task_name, json_payload, sql_text)

        # 3. additionally, save into file for reprocessing if needed:
        save_into_file(job_inst_id
            , job_inst_task_id
            , job_task_name
            , target_table
            , output_file_path
            , payload
            , response)


    except Exception as e:
        print(f"[{job_inst_id}] API processing failed || {e}")
        log_error(job_inst_id=job_inst_id
                  , task_name=job_task_name
                  , error_message=f"[{job_inst_id}] API processing failed: {e}"
                  , context="process_api_data()"
                  )
        log_job_task(job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
        raise

    log_job_task(job_inst_task_id, "succeeded")  # [metadata].[job_inst_task] table

def load_all_into_db(job_inst_id: int, job_inst_task_id: int, job_task_name:str, json_payload:str, sql_text:str):
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

def save_into_file(job_inst_id: int
                   , job_inst_task_id: int
                   , job_task_name:str
                   , target_table:str
                   , output_file_path:str
                   , payload: str               # request params
                   , response:requests.Response):
    # 2. additionally, save into file for reprocessing if needed:
    timestamp = datetime.now().strftime("%Y_%m_%d")
    file_path = f"{output_file_path}{target_table.split('.')[-1]}_{job_inst_id}_{timestamp}.json"
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


