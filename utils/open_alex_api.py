import requests
import pprint
import json
import time
from datetime import datetime
import os
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy import Table, MetaData, insert, event
from sqlalchemy.exc import SQLAlchemyError
from helper import get_engine_for_metadata, log_job_task, get_job_inst_task_info, log_info, log_error

"""
#####################################################################
POC of REST API call:
OpenAlex API with Cursor-Based Pagination
#####################################################################
The OpenAlex API provides scholarly data and uses cursor-based pagination. 
We send request to their /works endpoint with the cursor=* parameter to initiate pagination. 
Each response includes a meta.next_cursor value
, which we use in subsequent requests to retrieve the next set of results.
GET https://api.openalex.org/works?filter=publication_year:2020&per-page=100&cursor=*

{
  "meta": {
    "count": 8695857,
    "db_response_time_ms": 28,
    "page": null,
    "per_page": 100,
    "next_cursor": "IlsxNjA5MzcyODAwMDAwLCAnaHR0cHM6Ly9vcGVuYWxleC5vcmcvVzI0ODg0OTk3NjQnXSI="
  },
  "results": [
    // first page of results
  ]
}
#####################################################################
https://slack.engineering/evolving-api-pagination-at-slack/

logic:
openalex_fetch_and_save()
   ↳ loads config + table metadata (reflection)
   ↳ calls fetch_data_from_openalex(config) || fetches JSON data from the OpenAlex API using cursor-based pagination
        ↳ calls get_page(config, cursor) || request definitions
   ↳ calls save_results_to_file(all_results, config) || saves all JSON page responses into a single combined file
   ↳ calls load_data_into_db(config)
        ↳ calls openalex_load_all_into_db(config) || loads the file in chunks into the tgt table 
            ↳ truncate_table(config) 
            ↳ load_json_chunks(chunk, config) || load chunks from JSON file into the memory
            ↳ insert_into_db(chunk, config) || with fast_executemany = True (ODBC-level option for mysql+pyodbc))
#####################################################################
LOGGING NOTES: 
    ↳ print() is to see messages live inside Airflow logs.
    ↳ log_info() and log_error() capture everything into my metadata.log tables.
    ↳ log_job_task() updates task status (running, success, failed) in my metadata.job_inst & job_inst_task tables.
#####################################################################
"""
# === Configuration Class === #
class ApiConfig:
    def __init__(self, row: dict):
        self.job_inst_id = int(row.get("job_inst_id", 0))
        self.etl_step = row.get("etl_step", "E").strip()
        self.job_inst_task_id= int(row.get("job_inst_task_id", 0))
        self.job_task_name=row.get("job_task_name", "").strip()
        self.request_url = row.get("conn_str", "").strip()
        self.payload_json = row.get("payload_json", "").strip() if row.get("payload_json") else None
        self.target_table = row.get("tgt_fully_qualified_tbl_name", "").strip()
        self.sql_text = row.get("sql_text", "").strip() if row.get("sql_text") else None
        self.page_param_name = row.get("page_param_name", "page").strip()
        self.page_size = int(row.get("page_size", 100))
        self.cursor_param_name = row.get("cursor_param_name", "cursor").strip()
        self.cursor_path = row.get("cursor_path", "meta.next_cursor").strip()
        self.max_pages = int(row.get("max_pages", 2))
        self.output_folder = row.get('output_file_path')  # Folder only
        self.chunk_size = int(row.get("chunk_size", 100))
        self.is_full_load = False

        # keep the full original row
        self.raw = row

    def __str__(self):
        return f"ApiConfig({pprint.pformat(vars(self))})"

    # Use for developer view, debugging
    def __repr__(self):
        return self.__str__()

    @property
    def output_file_path(self):
        timestamp = datetime.now().strftime("%Y_%m_%d")
        filename = f"{self.target_table.split('.')[-1]}_{self.job_inst_id}_{timestamp}.json"
        return f"{self.output_folder}{filename}"

# === DB Setup === #
engine = get_engine_for_metadata()
metadata = MetaData()

# === Retry Decorator for API calls === #
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_page(config: ApiConfig, cursor):
    # Parse the payload if needed as it may be stored in the db as JSON string -e.g. '{"filter":"publication_year:2020"}'
    # If config.payload_json is a string, then json.loads() it into a dictionary.
    # Otherwise, if it’s already a dictionary, just use it as is.
    payload = json.loads(config.payload_json) if isinstance(config.payload_json, str) else config.payload_json

    params = {
        "filter": payload["filter"], # config.payload_json
        "per-page": config.page_size,
        config.cursor_param_name: cursor
    }
    print (params)
    print (config.request_url)
    response = requests.get(config.request_url, params=params)
    response.raise_for_status()
    return response.json()

# === Main ETL Methods === #
def openalex_fetch_and_save(job_inst_dict: dict):
    """
     Purpose:
         Process 'extract' task of a job instance.
         - REST API call
     Called from: process_task.process_extract_api_data()
     Parameters: job_inst_dict  - dict with job instance information
     """
    job_inst_id = job_inst_dict["job_inst_id"]
    job_inst_task_id = job_inst_dict["job_inst_task_id"]
    job_task_name = job_inst_dict["job_task_name"]

    # Mark task as "running"
    log_job_task(job_inst_task_id, "running")
    log_info(job_inst_id, job_task_name
             , "Starting OpenAlex fetch and save"
             , context="openalex_fetch_and_save")

    try:

        # Get API config parameters from metadata tables for this job-task-instance (i.e., job_task_inst_id)
        etl_step = "E" # this is extract
        job_task_inst_dict = get_job_inst_task_info(job_inst_id, etl_step, job_inst_task_id)
        print(job_task_inst_dict)

        # Initialize configuration object for this job-task-instance (i.e., job_task_inst_id)
        config = ApiConfig(job_task_inst_dict)

        # 1. Fetch and save data
        all_results = fetch_data_from_openalex(config)

        # 2. Save data to file
        save_results_to_file(all_results, config)

        # 3. After fetch and save, load data into the database
        load_data_into_db(config)

        # If everything is successful, mark the task as success
        log_job_task(job_inst_task_id, "succeeded")

    except Exception as e:
        # If anything goes wrong, log the error and mark the task as failed
        log_error(job_inst_id
                  , job_task_name
                  , f"Error in openalex_fetch_and_save: {str(e)}"
                  , context="openalex_fetch_and_save")
        log_job_task(job_inst_task_id, "failed")
        print(f"Error occurred: {e}")
        raise  # Re-raise the exception to propagate it


def fetch_data_from_openalex(config: ApiConfig):
    """
    Function to handle fetching of data from OpenAlex API.
    Handles pagination based on the cursor.
    """
    cursor = "*"  # Initial cursor value for OpenAlex API
    all_results = []  # List to collect all results

    # Fetch data page by page using cursor
    for page_num in range(config.max_pages):
        print(f"Fetching page {page_num + 1}")

        data = get_page(config, cursor)  # actual API call
        all_results.extend(data["results"])  # Append results to full list, keeping all of them in memory!

        # Get next cursor for the next API call
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            log_info(config.job_inst_id, config.job_task_name
                     , "No more cursor available, ending pagination.", context="fetch_data_from_openalex")
            print("No more cursor available, ending pagination.")
            break

        time.sleep(1)  # Sleep to avoid rate-limiting

    return all_results


def save_results_to_file(all_results, config: ApiConfig):
    """
    Save the fetched OpenAlex data to a JSON file.
    """
    os.makedirs(os.path.dirname(config.output_file_path), exist_ok=True)

    with open(config.output_file_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    #print("Config object fields:")
    #print (config)

    log_info(config.job_inst_id
             , config.job_task_name
             , f"Saved {len(all_results)} records to {config.output_file_path}"
             , context="save_results_to_file")
    print(f"Saved {len(all_results)} records to {config.output_file_path}")


def load_data_into_db(config: ApiConfig):
    """
    Load the JSON data into the database after fetching and saving.
    """
    log_info(config.job_inst_id
             , config.job_task_name
             , f"Starting loading JSON into DB table {config.target_table}"
             , context="load_data_into_db")
    print(f"Starting loading JSON into DB table {config.target_table}...")

    openalex_load_all_into_db(config)

    log_info(config.job_inst_id
             , config.job_task_name
             , f"Finished loading JSON into DB table {config.target_table}"
             , context="load_data_into_db")
    print(f"Finished loading JSON into DB table {config.target_table}...")


def openalex_load_all_into_db(config: ApiConfig):
    """
    Load data into the database in chunks:
    - Truncate the target table if `is_full_load` is True.
    - Load the data from the JSON file in chunks to avoid memory overload.
    - For each chunk, insert the data into the database.

    Arguments:
    config -- an instance of the ApiConfig class.
    """

    try:
        # Step 1: Truncate the table if it's a full load operation
        if config.is_full_load:
            log_info(job_inst_id=config.job_inst_id, task_name="openalex_load_all_into_db",
                     info_message=f"Starting full load. Truncating table {config.target_table}.",
                     context="openalex_load_all_into_db()")
            truncate_table(config)
            log_info(job_inst_id=config.job_inst_id, task_name="openalex_load_all_into_db",
                     info_message=f"Table {config.target_table} truncated successfully.",
                     context="openalex_load_all_into_db()")

        # Step 2: Load data in chunks from the JSON file and insert into the database
        log_info(job_inst_id=config.job_inst_id, task_name="openalex_load_all_into_db",
                 info_message=f"Starting to load data into table {config.target_table} in chunks of {config.chunk_size} records.",
                 context="openalex_load_all_into_db()")

        # Generator that yields data in chunks
        for chunk in load_json_chunks(config):
            log_info(job_inst_id=config.job_inst_id, task_name="openalex_load_all_into_db",
                     info_message=f"Inserting chunk of {len(chunk)} records into DB...",
                     context="openalex_load_all_into_db()")

            # Insert the current chunk into the database
            insert_into_db(chunk, config)

            log_info(job_inst_id=config.job_inst_id, task_name="openalex_load_all_into_db",
                     info_message=f"Inserted chunk of {len(chunk)} records into {config.target_table}.",
                     context="openalex_load_all_into_db()")

        log_info(job_inst_id=config.job_inst_id, task_name="openalex_load_all_into_db",
                 info_message=f"Data load completed successfully for table {config.target_table}.",
                 context="openalex_load_all_into_db()")

    except Exception as e:

        log_error(job_inst_id=config.job_inst_id, task_name="openalex_load_all_into_db",
                  error_message=f"An error occurred while loading data into {config.target_table}: {e}",
                  context="openalex_load_all_into_db()")
        raise


def truncate_table(config: ApiConfig):
    """
    This function truncates the target table in the database. It is called when the `is_full_load` flag is True.
    """
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            conn.execute(f"TRUNCATE TABLE {config.target_table}")
            trans.commit()
            log_info(job_inst_id=config.job_inst_id, task_name="truncate_table",
                     info_message=f"Successfully truncated table {config.target_table}.", context="truncate_table()")
    except Exception as e:
        trans.rollback()
        log_error(job_inst_id=config.job_inst_id, task_name="truncate_table",
                  error_message=f"Failed to truncate table {config.target_table}: {e}", context="truncate_table()")
        raise


def load_json_chunks(config: ApiConfig):
    """
    A generator function that loads data from the specified JSON file in chunks.

    filename -- The path to the JSON file containing the data.
    chunk_size -- The size of each chunk of data to yield.

    Yields:
    A chunk of data (a list of records) from the JSON file.
    """
    filename = config.output_file_path
    chunk_size = config.chunk_size

    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)  # Load the entire JSON file into memory

        # Yield chunks of data
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    except FileNotFoundError as e:
        log_error(job_inst_id=config.job_inst_id, task_name="load_json_chunks",
                  error_message=f"File {filename} not found: {e}", context="load_json_chunks()")
        raise  # Re-raise to stop further execution if the file is missing
    except json.JSONDecodeError as e:
        log_error(job_inst_id=config.job_inst_id, task_name="load_json_chunks",
                  error_message=f"Error decoding JSON file {filename}: {e}", context="load_json_chunks()")
        raise  # Re-raise if there's a problem parsing the JSON, i.e. if the file is corrupted or can't be parsed
    except Exception as e:
        log_error(job_inst_id=config.job_inst_id, task_name="load_json_chunks",
                  error_message=f"An error occurred while loading chunks from {filename}: {e}",
                  context="load_json_chunks()")
        raise  # Re-raise for general exceptions

# Enable fast_executemany globally for our engine
# Speed up batch insertions while using mssql+pyodbc
# This is ODBC-level optimization
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True

def get_nested_value(data, field_path, default_value=None)->list:
    """
    Safely fetch nested field value from dict, given 'field.subfield' path
    If the value doesn't exist, it will return `default_value`.
    """
    keys = field_path.split(".")
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default_value)  # Default value if key is missing
        else:
            return default_value  # Return default if the data isn't a dict or key is missing
    return data

TABLE_FIELD_MAPPING = {
    "openalex_works": ["id", "title", "doi", "publication_year", "host_venue", "type"],
    "openalex_authors": ["id", "display_name", "orcid", "works_count", "citation_count", "last_known_institution"],
    "openalex_sources": ["id", "display_name", "issn_l", "is_oa", "host_organization", "works_count"],
    "openalex_institutions": ["id", "display_name", "country_code", "type","ror","works_count"],
}

"""
#Future Enhancements:
#Parallelization: parallel processing or batch processing mechanisms 
# (e.g., asyncio or worker pools) to further speed up the insert process.
"""
def insert_into_db(chunk: list[dict], config: ApiConfig):
    try:
        # Retrieve target table from the database using reflection
        target_table = get_target_table_from_db(config)
        tbl_name = target_table.name
        fields = TABLE_FIELD_MAPPING.get(tbl_name, [])
        records_to_insert = []

        for rec in chunk:
            row = {}
            for field in fields:
                # Handle special case where we know it's nested
                if field == "host_venue":
                    row[field] = get_nested_value(rec, "primary_location.source.display_name")
                # authors
                elif field == "citation_count":
                    row[field] = get_nested_value(rec, "cited_by_count")
                elif field == "last_known_institution":
                    row[field] = get_nested_value(rec, "last_known_institutions.display_name")
                else:
                    row[field] = rec.get(field, None)  # fallback to top-level

            records_to_insert.append(row)

        try:
            with engine.begin() as conn:
                trans = conn.begin()
                conn.execute(insert(target_table), records_to_insert)
                trans.commit()

            # Log successful insertion
            log_info(job_inst_id=config.job_inst_id, task_name="insert_into_db",
                     info_message=f"Inserted {len(records_to_insert)} records into {config.target_table}.",
                     context="insert_into_db()")

        except SQLAlchemyError as e:
            # Log error and handle transaction failure
            trans.rollback()
            log_error(job_inst_id=config.job_inst_id, task_name="insert_into_db",
                      error_message=f"Failed to insert into {config.target_table}: {e}",
                      context="insert_into_db()")
            raise  # Re-raise exception to stop further processing

    except Exception as e:
        # Log unexpected errors
        log_error(job_inst_id=config.job_inst_id, task_name="insert_into_db",
                  error_message=f"An error occurred during insert operation: {e}",
                  context="insert_into_db()")
        raise  # Re-raise to stop execution in case of critical failure

# using reflection (autoload_with=engine) to load table definition at runtime from db.
def get_target_table_from_db(config: ApiConfig) -> Table:
    # Split schema and table name
    # since we need schema= explicitly
    schema, table_name = config.target_table.split('.')[-2], config.target_table.split('.')[-1]

    # Reflect table correctly
    return Table(
        table_name,
        metadata,
        schema=schema,
        autoload_with=engine
    )

