import shutil
from datetime import datetime
import os
import sys
from lib2to3.fixes.fix_except import find_excepts

import pandas as pd
from sqlalchemy import create_engine, text

from helper import get_engine_for_metadata, log_error, log_info, log_job_task
from open_alex_api import openalex_fetch_and_save
from reqres_api import reqres_fetch_and_save
from flat_file_utils import file_fetch_and_save
"""
#####################################################################
# A Processing that is based on the data source type.
# logic:
    process_extract_csv_file(row)                       # flat file 
     ↳ calls __truncate_table   # truncate tgt tbl 
     ↳ calls __move_file        # move file after processing        
    process_extract_task_mssql(row)                     # sql server 
    process_api_data(row)                               # REST API
     ↳ calls open_alex_api.openalex_fetch_and_save(row) # OpenAlex API with Cursor-Based Pagination
     ↳ calls reqres_api.reqres_fetch_and_save(row)      # Reqres API 
    process_transform_task_mssql()                      # Transform step via stored proc
    process_load_task_mssql()                           # Load step via stored proc

#####################################################################
LOGGING NOTES: 
    ↳ print() is to see messages live inside Airflow logs.
    ↳ log_info() and log_error() capture everything into my metadata.log tables.
    ↳ log_job_task() updates task status (running, success, failed) in my metadata.job_inst & job_inst_task tables.
#####################################################################
"""

def process_extract_csv_file(row: dict):
    file_fetch_and_save(row)

def process_extract_task_mssql(row: dict) -> int:
    """
    Purpose:
        Process 'extract' step of a job instance task.
        - For large datasets: export to CSV and bulk insert
        - For small datasets: load into temp table then copy to target

    Called from: extract()
    """

    job_inst_id = row.get("job_inst_id")
    job_inst_task_id = row.get("job_inst_task_id")
    job_task_name = row.get("job_task_name")
    conn_str = row.get("conn_str")
    sql_text = row.get("sql_text")
    sql_type = row.get("sql_type")
    target_table = row.get("tgt_fully_qualified_tbl_name")
    is_large = row.get("src_data_size", "small") == "large"
    del_temp_data = row.get("del_temp_data")
    is_full_load = row.get("is_full_load")
    incr_date = row.get("incr_date")
    incr_column = row.get("incr_column")

    log_job_task(job_inst_task_id, "running")   # [metadata].[job_inst_task] table

    engine_tgt = get_engine_for_metadata()  # Target
    engine_src = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")    # Source

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

    print(f"SQL Statement || {sql_text}")
    log_info(job_inst_id=job_inst_id
             , task_name=job_task_name
             , info_message=f"SQL Statement || {sql_text}"
             , context="process_extract_task_mssql()"
             )

    # if stored procedure :
    if sql_type.lower() == "proc":
        try:
            with engine_src.connect() as connection:
                trans = connection.begin()
                try:
                    sql_text = f"""
                                   EXEC {sql_text}  
                                       @p_job_inst_id = :param1,
                                       @p_job_inst_task_id = :param2
                               """
                    connection.execute(
                        text(sql_text),
                        {"param1": job_inst_id, "param2": job_inst_task_id}
                    )
                    trans.commit()
                except Exception as e:
                    # Rollback the transaction in case of an error
                    trans.rollback()
                    raise Exception(f"Transaction failed: {e}")

        except Exception as e:
            # [metadata].[log_dtl] table:
            log_error(job_inst_id=job_inst_id
                      , task_name="extract"
                      , error_message=str(e)
                      , context="process_extract_task_mssql()"
                      )
            log_job_task(job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
            raise
    # not a stored proc:
    else:

        try:
            with engine_src.connect() as conn_src:
                # add incremental date if it is not a full load:
                if not is_full_load:
                    sql_text = f"{sql_text}  and {incr_column} >= '{incr_date}'"

                df = pd.read_sql(text(sql_text), con=conn_src)
                if is_large:
                    # Write to CSV
                    # file_path = f"//XXX.XXX.XX.XX/shared/bulk_files/{target_table.replace('.', '_')}_{timestamp}.csv"

                    filename = f"{target_table.replace('.', '_')}_{timestamp}.csv"

                    # This is inside the container, but it's mapped to the Windows share
                    docker_shared_path = "/mnt/sql_shared/bulk_files/"
                    file_path = os.path.join(docker_shared_path, filename)

                    # Write CSV to shared folder
                    df.to_csv(file_path, index=False)

                    # The path as seen by SQL Server (UNC path)
                    sql_server_path = f"\\\\xxx.168.86.96\\shared\\bulk_files\\{filename}"

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
                             , context="process_extract_task_mssql()"
                             )

                    with engine_tgt.connect() as conn_tgt:
                        conn_tgt.execution_options(autocommit=True).execute(text(bulk_insert_sql))

                        # Fetch the row count from the result
                        result = conn_tgt.execute(text("SELECT @@ROWCOUNT AS inserted_rows"))
                        row_count = result.scalar()  # or: result.fetchone()["inserted_rows"]

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

                    # Small dataset and not stored proc: temp table flow
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

                        print(f"SQL Statement || {insert_sql}")
                        log_info(job_inst_id = job_inst_id
                            , task_name = job_task_name
                            , info_message = f"SQL Statement || {insert_sql}"
                            , context="process_extract_task_mssql()"
                            )

                        result = conn_tgt.execute(text(insert_sql))
                        rowcount = result.rowcount
                        print(f"Target table [{target_table}] || inserted {rowcount} rows")
                        log_info(job_inst_id = job_inst_id
                            , task_name = job_task_name
                            , info_message = f"Target table [{target_table}] || inserted {rowcount} rows"
                            , context="process_extract_task_mssql()"
                            )

                        # Drop temp table if requested
                        if del_temp_data:
                            try:
                                conn_tgt.execute(text(f"DROP TABLE {temp_table}"))
                                print(f"Dropped temp table || {temp_table}")
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

def process_api_data(row: dict):

    data_source_name = row["data_source_name"]
    if data_source_name.lower() == "openalex":
        openalex_fetch_and_save(row)
    elif data_source_name.lower() == "reqres":
        reqres_fetch_and_save(row)


def process_transform_task_mssql(
        job_inst_id: int,
        job_inst_task_id: int,
        conn_str: str,
        sql_text: str,
) :

    """
    Purpose:
        Process 'transform' step of a job instance task.
        - via stored proc

    Called from: transform()
    """
    log_job_task(job_inst_task_id, "running")  # [metadata].[job_inst_task] table

    engine = get_engine_for_metadata()  # Target

    log_info(job_inst_id=job_inst_id
             , task_name="transform"
             , info_message=f"target || {engine}"
             , context="process_transform_task_mssql()"
             )
    print(f"target || {engine}")

    try:
            with engine.connect() as connection:
                trans = connection.begin()
                try:

                    proc_name = sql_text

                    sql_text = f"""
                                    EXEC {proc_name}  
                                        @p_job_inst_id = :param1,
                                        @p_job_inst_task_id = :param2
                                """
                    connection.execute(
                        text(sql_text),
                        {"param1": job_inst_id, "param2": job_inst_task_id}
                    )

                    trans.commit()

                except Exception as e:
                    # Rollback the transaction in case of an error
                    trans.rollback()
                    raise Exception(f"Transaction failed: {e}")

                #report success:
                log_job_task(job_inst_task_id, "succeeded") # [metadata].[job_inst_task] table

    except Exception as e:
        # [metadata].[log_dtl] table:
        log_error(job_inst_id=job_inst_id
                  , task_name="transform"
                  , error_message=str(e)
                  , context="process_transform_task_mssql()"
                  )
        log_job_task(job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
        raise

def process_load_task_mssql(
        job_inst_id: int,
        job_inst_task_id: int,
        conn_str: str,
        sql_text: str,
) :

    """
    Purpose:
        Process 'transform' step of a job instance task.
        - via stored proc

    Called from: load()
    """
    log_job_task(job_inst_task_id, "running")  # [metadata].[job_inst_task] table

    engine = get_engine_for_metadata()  # Target

    log_info(job_inst_id=job_inst_id
             , task_name="load"
             , info_message=f"target || {engine}"
             , context="process_load_task_mssql()"
             )
    print(f"target || {engine}")

    try:
            with engine.connect() as connection:
                trans = connection.begin()
                try:

                    proc_name = sql_text

                    sql_text = f"""
                                    EXEC {proc_name}  
                                        @p_job_inst_id = :param1,
                                        @p_job_inst_task_id = :param2
                                """
                    connection.execute(
                        text(sql_text),
                        {"param1": job_inst_id, "param2": job_inst_task_id}
                    )

                    trans.commit()

                except Exception as e:
                    # Rollback the transaction in case of an error
                    trans.rollback()
                    raise Exception(f"Transaction failed: {e}")

                #report success:
                log_job_task(job_inst_task_id, "succeeded") # [metadata].[job_inst_task] table
    except Exception as e:
        # [metadata].[log_dtl] table:
        log_error(job_inst_id=job_inst_id
                  , task_name="load"
                  , error_message=str(e)
                  , context="process_load_task_mssql()"
                  )
        log_job_task(job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
        raise
