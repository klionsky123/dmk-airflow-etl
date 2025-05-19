import shutil
from datetime import datetime
import pprint
import os
import csv
import sys
from helper import get_engine_for_metadata, log_error, log_info, log_job_task, get_job_inst_task_info
import pandas as pd
from sqlalchemy import Table, MetaData, insert, event, text
from sqlalchemy.exc import SQLAlchemyError

"""
#####################################################################
POC of csv file load:
#####################################################################

logic:
file_fetch_and_save()
   ↳ loads config + table metadata (reflection)
   ↳ calls _load_data_into_db(config)
            ↳ _truncate_table(config)
            if file is large: 
                ↳ _load_csv_chunks(chunk, config) || load chunks from file into the memory
                ↳ _insert_into_db(chunk, config) || with fast_executemany = True (ODBC-level option for mysql+pyodbc))
            if file is small:
                ↳ _load_small_file(config)
            ↳ _move_file()                         || archive processed file
#####################################################################
LOGGING NOTES: 
    ↳ print() is to see messages live inside Airflow logs.
    ↳ log_info() and log_error() capture everything into my metadata.log tables.
    ↳ log_job_task() updates task status (running, success, failed) in my metadata.job_inst & job_inst_task tables.
#####################################################################
"""


# === Configuration Class === #
class FlatFileConfig:
    def __init__(self, row: dict):
        self.job_inst_id = int(row.get("job_inst_id", 0))
        self.etl_step = row.get("etl_step", "E").strip()
        self.job_inst_task_id = int(row.get("job_inst_task_id", 0))
        self.job_task_name = "file_fetch_and_save"
        self.target_table = row.get("tgt_fully_qualified_tbl_name", "").strip()
        self.sql_text = row.get("sql_text", "").strip() if row.get("sql_text") else None
        self.docker_shared_path = "/opt/airflow/tmp/"
        self.file_name = row.get("file_path", "").strip()
        self.chunk_size = 100
        self.is_full_load = row.get("is_full_load", True)
        self.file_size = row.get("src_data_size")
        self.is_large = row.get("src_data_size", "small") == "large"

        # keep the full original row
        self.raw = row

    def __str__(self):
        return f"FlatFileConfig({pprint.pformat(vars(self))})"

    # Use for developer view, debugging
    def __repr__(self):
        return self.__str__()

    @property
    def file_path_and_name(self):
        # This is inside the Docker container, but it's mapped to the Windows share
        # file_path_and_name = os.path.join(self.docker_shared_path, self.file_name)
        return f"{self.docker_shared_path}{self.file_name}"


# raw.city table:
TABLE_FIELD_MAPPING = {
    "city": ["city_id", "name", "locale_id", "date_created", "master_record_id"],
}

# === DB Setup === #
engine = get_engine_for_metadata()
metadata = MetaData()


# Enable fast_executemany globally for our engine
# Speed up batch insertions while using mssql+pyodbc
# This is ODBC-level optimization
@event.listens_for(engine, "before_cursor_execute")
def _receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True


def file_fetch_and_save(job_inst_dict: dict):
    """
     Purpose:
         Process 'extract' task of a job instance.
         - bulk insert

     Called from: process_task.process_extract_csv_file()
     Parameters: job_inst_dict  - dict with job instance information
     """
    job_inst_id = job_inst_dict["job_inst_id"]
    job_inst_task_id = job_inst_dict["job_inst_task_id"]
    job_task_name = job_inst_dict["job_task_name"]

    # Mark task as "running"
    log_job_task(job_inst_task_id, "running")

    try:

        # Get config parameters from metadata tables for this job-task-instance (i.e., job_task_inst_id)
        etl_step = "E"  # this is extract
        job_task_inst_dict = get_job_inst_task_info(job_inst_id, etl_step, job_inst_task_id)
        print(job_task_inst_dict)

        # Initialize configuration object for this job-task-instance (i.e., job_task_inst_id)
        config = FlatFileConfig(job_task_inst_dict)

        log_info(job_inst_id, job_task_name
                 , f"Starting csv file load || {config.file_path_and_name}"
                 , context="file_fetch_and_save")

        # load data into the database
        _load_data_into_db(config)

        # move file after processing to 'processed' directory
        print(config.file_path_and_name)
        if os.path.exists(config.file_path_and_name):
            _move_file(job_inst_id=config.job_inst_id, file_path=config.file_path_and_name, target_dir="processed")

        # If everything is successful, mark the task as success
        log_job_task(job_inst_task_id, "succeeded")

    except Exception as e:
        # If anything goes wrong, log the error and mark the task as failed
        log_error(job_inst_id
                  , job_task_name
                  , f"Error in file_fetch_and_save: {str(e)}"
                  , context="file_fetch_and_save()")
        log_job_task(job_inst_task_id, "failed")
        print(f"Error occurred: {e}")
        raise  # Re-raise the exception to propagate it


def _load_data_into_db(config: FlatFileConfig):
    """
    Load data into the database in chunks:
    - Truncate the target table if `is_full_load` is True.
    - Load the data from the file in chunks to avoid memory overload.
    - For each chunk, insert the data into the database.

    Arguments:
    config -- an instance of the FlatFileConfig class.
    """

    try:
        # Step 1: Truncate the table if it's a full load operation
        if config.is_full_load:
            _truncate_table(config)

        # Step 2: load into db based on the file size
        if config.is_large:  # large file

            # Load data in chunks from the file and insert into the database
            log_info(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                     info_message=f"File size set to {config.file_size}. Starting to load data into table {config.target_table} in chunks of {config.chunk_size} records.",
                     context="_load_data_into_db()")

            # Generator that yields data in chunks
            for chunk in _load_csv_chunks(config):
                # Insert the current chunk into the database
                _insert_into_db(chunk, config)

        else:  # small file

            log_info(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                     info_message=f"File size set to {config.file_size}. Starting to bulk-load data into table {config.target_table} ",
                     context="_load_data_into_db()")
            _load_small_file(config)

        # # Mark task as success:
        log_info(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                 info_message=f"Successfully loaded data into table {config.target_table}.",
                 context="_load_data_into_db()")


    except Exception as e:

        log_error(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                  error_message=f"An error occurred while loading data into {config.target_table}: {e}",
                  context="_load_data_into_db()")
        # on error, move file  to 'error' directory
        if os.path.exists(config.file_path_and_name):
            _move_file(job_inst_id=config.job_inst_id, file_path=config.file_path_and_name, target_dir="error")

        raise


def _load_small_file(config: FlatFileConfig):
    """
     Purpose:
         Process 'extract' task of a job instance.
         - bulk insert

     Called from: process_task.process_extract_csv_file()
     """

    job_inst_id = config.job_inst_id
    job_task_name = config.job_task_name
    target_table = config.target_table

    # The path as seen by SQL Server (UNC path)
    sql_server_path = f"\\\\192.168.86.96\\shared\\bulk_files\\{config.file_name}"

    # Append a query to capture the row count after the bulk insert
    bulk_insert_sql = f"""
                            BULK INSERT {target_table}
                            FROM '{sql_server_path}'
                            WITH (
                                FIRSTROW = 2,
                                FIELDTERMINATOR = ',',
                                ROWTERMINATOR = '\\n',
                                TABLOCK
                            );

                        """

    print(bulk_insert_sql)
    log_info(job_inst_id=job_inst_id
             , task_name=job_task_name
             , info_message=f"SQL Statement || {bulk_insert_sql}"
             , context="load_small_file()"
             )

    # insert:
    engine_tgt = get_engine_for_metadata()  # Target
    with engine_tgt.connect() as conn_tgt:
        try:
            conn_tgt.execution_options(autocommit=True).execute(text(bulk_insert_sql))

            # Fetch the row count from the result
            result = conn_tgt.execute(text("SELECT @@ROWCOUNT AS inserted_rows"))
            row_count = result.scalar()  # or: result.fetchone()["inserted_rows"]

            print(f"Number of rows inserted: {row_count}")
            log_info(job_inst_id=job_inst_id
                     , task_name=job_task_name
                     , info_message=f"Successfully inserted into target table {target_table} || {row_count} rows"
                     , context="load_small_file()"
                     )

        except Exception as e:
            log_error(job_inst_id=job_inst_id, task_name=job_task_name,
                      error_message=f"Failed to insert into table {target_table}: {e}",
                      context="load_small_file()")
        ###################################################################

    return row_count


def _truncate_table(config: FlatFileConfig):
    """
    This function truncates the target table in the database. It is called when the `is_full_load` flag is True.
    """
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            conn.execute(f"TRUNCATE TABLE {config.target_table}")
            trans.commit()
            log_info(job_inst_id=config.job_inst_id, task_name="truncate_table",
                     info_message=f"Successfully truncated target table {config.target_table}.",
                     context="truncate_table()")
    except Exception as e:
        trans.rollback()
        log_error(job_inst_id=config.job_inst_id, task_name="truncate_table",
                  error_message=f"Failed to truncate table {config.target_table}: {e}", context="truncate_table()")
        raise


def _move_file(job_inst_id: int, file_path: str, target_dir: str):
    """
    This function moves file_path file after processing to:
    target_dir = "error" in case of error
    target_dir = "processed" in cased of success

    """
    if os.path.exists(file_path):
        processed_dir = os.path.join(os.path.dirname(file_path), target_dir)
        os.makedirs(processed_dir, exist_ok=True)  # Ensure the directory exists

        # Split the filename and add a timestamp like file_20250430_143522.csv.
        base_name = os.path.basename(file_path)
        name, ext = os.path.splitext(base_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # add prefix 'err_' for error:
        new_filename = f"{'err_' if target_dir == 'error' else ''}{name}_{timestamp}{ext}"
        new_path = os.path.join(processed_dir, new_filename)

        shutil.move(file_path, new_path)
        print(f"Moved file to: {new_path}")

        log_info(job_inst_id=job_inst_id, task_name="file_fetch_and_save",
                 info_message=f"Moved file to: {new_path}.", context="_move_file")


def _load_csv_chunks(config: FlatFileConfig):
    """
    A generator function that loads data from a CSV file in chunks.
    'Yield' lets a function return data one piece at a time instead of all at once.

    config.output_file_path -- Path to the CSV file.
    config.chunk_size -- Number of rows per chunk.

    Yields:
    A chunk of data (a list of dictionaries) from the CSV file.
    💡 Why use yield?
    * Memory-efficient: especially useful when reading large files (like CSVs or JSON).
    * Lazy evaluation: processes data as needed without loading it all at once.
    """
    filename = config.file_path_and_name
    chunk_size = config.chunk_size

    try:
        with open(filename, mode="r", encoding="latin1") as f:
            reader = csv.DictReader(f)  # parse rows as dictionaries (column names as keys).
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) == chunk_size:
                    yield chunk  # pause the function here and return chunk, but remember where we left off so we can resume later :)
                    chunk = []
            if chunk:
                yield chunk  # yield remaining rows if any

    except FileNotFoundError as e:
        log_error(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                  error_message=f"File {filename} not found: {e}", context="_load_csv_chunks()")
        raise
    except Exception as e:
        log_error(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                  error_message=f"An error occurred while loading chunks from {filename}: {e}",
                  context="_load_csv_chunks()")
        raise


def _insert_into_db(chunk: list[dict], config: FlatFileConfig):
    try:
        # Retrieve target table from the database using reflection
        target_table = _get_target_table_from_db(config)
        tbl_name = target_table.name
        fields = TABLE_FIELD_MAPPING.get(tbl_name, [])
        records_to_insert = []

        for rec in chunk:
            row = {}
            for field in fields:
                row[field] = rec.get(field, None)

            records_to_insert.append(row)

        try:
            with engine.begin() as conn:
                trans = conn.begin()
                conn.execute(insert(target_table), records_to_insert)
                trans.commit()

            # Log successful insertion
            log_info(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                     info_message=f"Inserted {len(records_to_insert)} records into {config.target_table}.",
                     context="_insert_into_db()")

        except SQLAlchemyError as e:
            # Log error and handle transaction failure
            trans.rollback()
            log_error(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                      error_message=f"Failed to insert into {config.target_table}: {e}",
                      context="_insert_into_db()")
            raise  # Re-raise exception to stop further processing


    except Exception as e:
        # Log unexpected errors
        log_error(job_inst_id=config.job_inst_id, task_name="file_fetch_and_save",
                  error_message=f"An error occurred during insert operation: {e}",
                  context="_insert_into_db()")
        raise  # Re-raise to stop execution in case of critical failure


# using reflection (autoload_with=engine) to load table definition at runtime from db.
def _get_target_table_from_db(config: FlatFileConfig) -> Table:
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
