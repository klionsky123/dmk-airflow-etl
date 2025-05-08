import shutil
from datetime import datetime
import os
import sys
from lib2to3.fixes.fix_except import find_excepts

import pandas as pd
from sqlalchemy import create_engine, text

from helper import get_engine_for_metadata, log_error, log_info, log_job_task, complete_job
from open_alex_api import openalex_fetch_and_save
from reqres_api import reqres_fetch_and_save
from flat_file_utils import file_fetch_and_save
"""
#####################################################################
# A Processing that is based on the data source type.
# logic:
process(self) 
    ↳ calls _extract_csv_file()                        # flat file 
     ↳ calls _truncate_table                           # truncate tgt tbl 
     ↳ calls _move_file                                # move file after processing        
    ↳ calls _extract_mssql()                           # sql server 
    ↳ calls _extract_api_data()                        # REST API
     ↳ calls open_alex_api.openalex_fetch_and_save(row) # OpenAlex API with Cursor-Based Pagination
     ↳ calls reqres_api.reqres_fetch_and_save(row)      # Reqres API 
    ↳ calls _transform_mssql()                         # Transform step via stored proc
    ↳ calls _load_mssql()                              # Load step via stored proc

#####################################################################
LOGGING NOTES: 
    ↳ print() is to see messages live inside Airflow logs.
    ↳ log_info() and log_error() capture everything into my metadata.log tables.
    ↳ log_job_task() updates task status (running, success, failed) in my metadata.job_inst & job_inst_task tables.
#####################################################################
"""
class JobTask:
    def __init__(self, row):
        self.row = row
        self.job_inst_id = row["job_inst_id"]
        self.job_inst_task_id = row["job_inst_task_id"]
        self.job_task_name = row["job_task_name"]
        self.source_table = row.get("src_fully_qualified_tbl_name")
        self.target_table = row.get("tgt_fully_qualified_tbl_name")
        self.conn_type = row.get("conn_type", "").lower()
        self.db_type = row.get("db_type", "").lower() if row.get("db_type") else None
        self.file_path = row.get("file_path")
        self.etl_step = row.get("etl_step")
        self.engine_tgt = get_engine_for_metadata()  # Target
        ################# DATABASE-RELATED: ##############
        self.sql_text = row.get("sql_text")
        self.sql_type = row.get("sql_type")
        self.conn_str = row.get("conn_str")
        self.is_large = row.get("src_data_size", "small") == "large"
        self.del_temp_data = row.get("del_temp_data")
        self.is_full_load = row.get("is_full_load")
        self.incr_date = row.get("incr_date")
        self.incr_column = row.get("incr_column")
        ################## REST API RELATED: ########################
        self.data_source_name = row["data_source_name"]

    def process(self):
        print(f"Started task || {self.job_task_name} || {self.source_table} --> {self.target_table}")
        log_info(job_inst_id=self.job_inst_id,
                 task_name=self.job_task_name,
                 info_message=f"Started task || {self.job_task_name} || {self.source_table} --> {self.target_table}",
                 context="JobTask.process()")

        try:
            if self.etl_step == "E":
                if self.conn_type == "db":
                    if "mssql" in self.db_type:
                        self._extract_mssql()
                elif "api" in self.conn_type:
                        self._extract_api_data()
                elif self.conn_type == "file":
                        self._extract_csv_file()
                elif self.conn_type == "parquet":
                        self._parquet_file()
                else:
                    print(f"[{self.job_inst_id}] Unknown connection_type: {self.conn_type}")
            elif self.etl_step == "T":
                return self._transform_mssql()
            elif self.etl_step == "L":
                return self._load_mssql()

        except Exception as e:
            print(f"[{self.job_inst_id}] Extract task [{self.job_task_name}] failed: {e}")
            log_error(self.job_inst_id, "extract", str(e), "JobTask.process()")
            raise

        log_job_task(self.job_inst_task_id, "succeeded")
        print(f"{self.job_task_name} task succeeded")

        print(f"Finished task || {self.job_task_name}")
        log_info(job_inst_id=self.job_inst_id,
                 task_name=self.job_task_name,
                 info_message=f"Finished task || {self.job_task_name} ",
                 context="JobTask.process()")
        return None

    def _extract_mssql(self):
        """
           Purpose:
               Process 'extract' step of a job instance task.
               - For large datasets: export to CSV and bulk insert
               - For small datasets: load into temp table then copy to target

           Called from: extract()
           """
        log_job_task(self.job_inst_task_id, "running")  # [metadata].[job_inst_task] table


        log_info(job_inst_id=self.job_inst_id
                 , task_name=self.job_task_name
                 , info_message=f"target || {self.engine_tgt}"
                 , context="process_extract_task_mssql()"
                 )

        print(f"target || {self.engine_tgt}")

        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        print(f"SQL Statement || {self.sql_text}")
        log_info(job_inst_id=self.job_inst_id
                 , task_name=self.job_task_name
                 , info_message=f"SQL Statement || {self.sql_text}"
                 , context="process_extract_task_mssql()"
                 )

        # if stored procedure :
        if self.sql_type.lower() == "proc":

            # a SQLAlchemy-compatible URL: "mssql+pyodbc://uname:pass@ip_address/db_name?driver=ODBC+Driver+17+for+SQL+Server"
            # (Replaced spaces in the driver name with + )
            _engine_src = create_engine(f"mssql+pyodbc:///?odbc_connect={self.conn_str}")  # Source

            print(f"source || {_engine_src}")
            log_info(job_inst_id=self.job_inst_id
                     , task_name=self.job_task_name
                     , info_message=f"source || {_engine_src}"
                     , context="process_extract_task_mssql()"
                     )

            try:
                with _engine_src.connect() as connection:
                    trans = connection.begin()
                    try:
                        _sql_text = f"""
                                          EXEC {self.sql_text}  
                                              @p_job_inst_id = :param1,
                                              @p_job_inst_task_id = :param2
                                      """
                        connection.execute(
                            text(_sql_text),
                            {"param1": self.job_inst_id, "param2": self.job_inst_task_id}
                        )
                        trans.commit()
                    except Exception as e:
                        # Rollback the transaction in case of an error
                        trans.rollback()
                        raise Exception(f"Transaction failed: {e}")

            except Exception as e:
                # [metadata].[log_dtl] table:
                log_error(job_inst_id=self.job_inst_id
                          , task_name="extract"
                          , error_message=str(e)
                          , context="process_extract_task_mssql()"
                          )
                log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
                raise
        # not a stored proc:
        else:
            # panda style conn_str: 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=xxx;DATABASE=xxx;UID=xxx;PWD=xxx'
            _engine_src = create_engine(f"{self.conn_str}")  # Source

            print(f"source || {_engine_src}")
            log_info(job_inst_id=self.job_inst_id
                     , task_name=self.job_task_name
                     , info_message=f"source || {_engine_src}"
                     , context="process_extract_task_mssql()"
                     )

            try:
                with _engine_src.connect() as conn_src:
                    # add incremental date if it is not a full load:
                    sql_text = f"{self.sql_text}  and {self.incr_column} >= '{self.incr_date}'" if not self.is_full_load else self.sql_text
                    print(sql_text)
                    df = pd.read_sql(text(sql_text), con=conn_src)
                    if self.is_large:
                        # Write to CSV
                        # file_path = f"//XXX.XXX.XX.XX/shared/bulk_files/{target_table.replace('.', '_')}_{timestamp}.csv"

                        filename = f"{self.target_table.replace('.', '_')}_{timestamp}.csv"

                        # This is inside the container, but it's mapped to the Windows share
                        docker_shared_path = "/mnt/sql_shared/bulk_files/"
                        file_path = os.path.join(docker_shared_path, filename)

                        # Write CSV to shared folder
                        df.to_csv(file_path, index=False)

                        # The path as seen by SQL Server (UNC path)
                        sql_server_path = f"\\\\192.168.86.96\\shared\\bulk_files\\{filename}"

                        # Append a query to capture the row count after the bulk insert
                        bulk_insert_sql = f"""
                               BULK INSERT {self.target_table}
                               FROM '{sql_server_path}'
                               WITH (
                                   FIRSTROW = 2,
                                   FIELDTERMINATOR = ',',
                                   ROWTERMINATOR = '\\n',
                                   TABLOCK
                               );
                           """

                        print(bulk_insert_sql)
                        log_info(job_inst_id=self.job_inst_id
                                 , task_name=self.job_task_name
                                 , info_message=f"SQL Statement || {bulk_insert_sql}"
                                 , context="process_extract_task_mssql()"
                                 )

                        with self.engine_tgt.connect() as conn_tgt:
                            conn_tgt.execution_options(autocommit=True).execute(text(bulk_insert_sql))

                            # Fetch the row count from the result
                            result = conn_tgt.execute(text("SELECT @@ROWCOUNT AS inserted_rows"))
                            row_count = result.scalar()  # or: result.fetchone()["inserted_rows"]

                            print(f"Number of rows inserted: {row_count}")
                            log_info(job_inst_id=self.job_inst_id
                                     , task_name=self.job_task_name
                                     , info_message=f"Target table [{self.target_table}] || inserted {row_count} rows"
                                     , context="process_extract_task_mssql()"
                                     )

                        # Delete temp file if needed
                        if self.del_temp_data and os.path.exists(file_path):
                            os.remove(file_path)

                            print(f"Dropped {file_path}")
                            log_info(job_inst_id=self.job_inst_id
                                     , task_name=self.job_task_name
                                     , info_message=f"Dropped {file_path}"
                                     , context="process_extract_task_mssql()"
                                     )

                    else:

                        # Small dataset and not stored proc: temp table flow
                        # staging the data in a temporary table first, a common pattern in ETL for validation

                        # just getting the last part, which is the actual table name
                        temp_table = f"temp_{self.target_table.split('.')[-1]}_{self.job_inst_id}"
                        print(f"Temp table: {temp_table}")

                        with self.engine_tgt.connect() as conn_tgt:
                            # Create empty temp table
                            # df.head(0) gives the column structure without rows.
                            # if_exists="replace" drops and recreates the table if it already exists.
                            df.head(0).to_sql(temp_table, con=conn_tgt, if_exists="replace", index=False)

                            # Insert data
                            df.to_sql(temp_table, con=conn_tgt, if_exists="append", index=False, method="multi")

                            # Move to target table
                            insert_sql = f"INSERT INTO {self.target_table} SELECT * FROM {temp_table}"

                            print(f"SQL Statement || {insert_sql}")
                            log_info(job_inst_id=self.job_inst_id
                                     , task_name=self.job_task_name
                                     , info_message=f"SQL Statement || {insert_sql}"
                                     , context="process_extract_task_mssql()"
                                     )

                            result = conn_tgt.execute(text(insert_sql))
                            rowcount = result.rowcount
                            print(f"Target table [{self.target_table}] || inserted {rowcount} rows")
                            log_info(job_inst_id=self.job_inst_id
                                     , task_name=self.job_task_name
                                     , info_message=f"Target table [{self.target_table}] || inserted {rowcount} rows"
                                     , context="process_extract_task_mssql()"
                                     )

                            # Drop temp table if requested
                            if self.del_temp_data:
                                try:
                                    conn_tgt.execute(text(f"DROP TABLE {temp_table}"))
                                    print(f"Dropped temp table || {temp_table}")
                                    log_info(job_inst_id=self.job_inst_id
                                             , task_name=self.job_task_name
                                             , info_message=f"Dropped temp table || {temp_table}"
                                             , context="process_extract_task_mssql()"
                                             )
                                except Exception as drop_err:
                                    print(f"Warning: Failed to drop temp table {temp_table}: {drop_err}")
                                    log_error(job_inst_id=self.job_inst_id
                                              , task_name=self.job_task_name
                                              , error_message=f"Failed to drop temp table {temp_table}: {drop_err}"
                                              , context="process_extract_task_mssql()"
                                              )
                                    log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table

                    return len(df)

            except Exception as e:
                # [metadata].[log_dtl] table:
                log_error(job_inst_id=self.job_inst_id
                          , task_name=self.job_task_name
                          , error_message=str(e)
                          , context="process_extract_task_mssql()"
                          )
                log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
                raise


    def _extract_api_data(self):
        _extract_api_data=None
        data_source_name = self.data_source_name
        if data_source_name.lower() == "openalex":
            _extract_api_data = openalex_fetch_and_save(self.row)
        elif data_source_name.lower() == "reqres":
            _extract_api_data = reqres_fetch_and_save(self.row)
        return _extract_api_data

    def _extract_csv_file(self):
        return file_fetch_and_save(self.row)


    def _parquet_file(self):
        df = pd.read_parquet(self.file_path)
        print(f"[{self.job_inst_id}] Parquet loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        return df.shape[0]



    def _transform_mssql(self) :

        """
        Purpose:
            Process 'transform' step of a job instance task.
            - via stored proc

        Called from: transform()
        """
        log_job_task(self.job_inst_task_id, "running")  # [metadata].[job_inst_task] table

        log_info(job_inst_id=self.job_inst_id
                 , task_name="transform"
                 , info_message=f"target || {self.engine_tgt}"
                 , context="_transform_mssql()"
                 )
        print(f"target || {self.engine_tgt}")

        try:
                with self.engine_tgt.connect() as connection:
                    trans = connection.begin()
                    try:

                        proc_name = self.sql_text

                        sql_text = f"""
                                        EXEC {proc_name}  
                                            @p_job_inst_id = :param1,
                                            @p_job_inst_task_id = :param2
                                    """
                        connection.execute(
                            text(sql_text),
                            {"param1": self.job_inst_id, "param2": self.job_inst_task_id}
                        )

                        trans.commit()

                    except Exception as e:
                        # Rollback the transaction in case of an error
                        trans.rollback()
                        raise Exception(f"Transaction failed: {e}")

                    #report success:
                    log_job_task(self.job_inst_task_id, "succeeded") # [metadata].[job_inst_task] table

        except Exception as e:
            # [metadata].[log_dtl] table:
            log_error(job_inst_id=self.job_inst_id
                      , task_name="transform"
                      , error_message=str(e)
                      , context="_transform_mssql()"
                      )
            log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
            raise

    def _load_mssql(self) :

        """
        Purpose:
            Process 'transform' step of a job instance task.
            - via stored proc

        Called from: load()
        """
        log_job_task(self.job_inst_task_id, "running")  # [metadata].[job_inst_task] table


        log_info(job_inst_id=self.job_inst_id
                 , task_name="load"
                 , info_message=f"target || {self.engine_tgt}"
                 , context="_load_mssql()"
                 )
        print(f"target || {self.engine_tgt}")

        try:
                with self.engine_tgt.connect() as connection:
                    trans = connection.begin()
                    try:

                        proc_name = self.sql_text

                        sql_text = f"""
                                        EXEC {proc_name}  
                                            @p_job_inst_id = :param1,
                                            @p_job_inst_task_id = :param2
                                    """
                        connection.execute(
                            text(sql_text),
                            {"param1": self.job_inst_id, "param2": self.job_inst_task_id}
                        )

                        trans.commit()

                    except Exception as e:
                        # Rollback the transaction in case of an error
                        trans.rollback()
                        raise Exception(f"Transaction failed: {e}")

                    #report success:
                    log_job_task(self.job_inst_task_id, "succeeded") # [metadata].[job_inst_task] table
        except Exception as e:
            # [metadata].[log_dtl] table:
            log_error(job_inst_id=self.job_inst_id
                      , task_name="load"
                      , error_message=str(e)
                      , context="_load_mssql()"
                      )
            log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
            raise
