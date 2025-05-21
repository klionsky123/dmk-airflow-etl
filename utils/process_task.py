from datetime import datetime
import os
import inspect
import json
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from helper import log_error, log_info, log_job_task, get_engine_for_metadata
from open_alex_api import openalex_fetch_and_save
from reqres_api import reqres_fetch_and_save
from flat_file_utils import file_fetch_and_save
from kafka_consumer import KafkaETLConsumer
from kafka.errors import UnknownTopicOrPartitionError
from kafka_producer_client import KafkaProducerClient
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
        self.job_type = row.get("job_type").lower() if row.get("job_type") else None
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
        ################## KAFKA RELATED: ########################
        self.topic = row['kafka_topic']
        # for consumer, store in db as {"pattern": "test-.*"}, first, convert it from str to json
        self.topic_pattern = json.loads(row['kafka_topic_pattern']).get("pattern", "Key not found") if row['kafka_topic_pattern'] else None
        self.bootstrap = row.get('kafka_bootstrap_servers', 'kafka:9092')
        self.group_id = row.get('kafka_group_id', 'etl-group')
        self.is_kafka_consumer = row.get("is_kafka_consumer")
        self.kafka_batch_record_size = row.get("kafka_batch_record_size")
        self.max_messages = row.get("max_message_num") # Set limit if we don’t want to consume forever
        # keep log entries sequential during parallel task processing: ################################
        self.task_thread_num = row.get('task_thread_num') if row.get("task_thread_num") else 1
        self.task_logging_seq = row.get("sequence_num")  if row.get("sequence_num") else 100

    def process(self):
        if self.etl_step.upper() == "E":
            logging_step = 120 + self.task_logging_seq
        elif self.etl_step.upper() == "T":
            logging_step = 220 + self.task_logging_seq
        elif self.etl_step.upper() == "L":
            logging_step = 320 + self.task_logging_seq
        else:
            logging_step = self.task_logging_seq

        _info_msg = f"[#{self.task_thread_num}] Started task || {self.job_task_name} || {self.source_table} --> {self.target_table}"
        print(_info_msg)
        log_info(
            job_inst_id=self.job_inst_id,
            task_name=self.job_task_name,
            info_message=_info_msg,
            context="JobTask.process()",
            logging_seq=logging_step
        )

        try:
            if self.job_type == "non-etl":
                if self.conn_type == "kafka":
                    self._kafka_produce_messages()
            else:   # etl jobs
                if self.etl_step == "E":
                    if self.conn_type == "db":
                        if "mssql" in self.db_type:
                            self._extract_mssql(logging_step) #need logging_step to order parallel processing log entries
                    elif "api" in self.conn_type:
                            self._extract_api_data()
                    elif self.conn_type == "file":
                            self._extract_csv_file()
                    elif self.conn_type == "parquet":
                            self._parquet_file()
                    elif self.conn_type == "kafka" and self.is_kafka_consumer:
                            self._extract_kafka_data()
                    else:
                        print(f"[{self.job_inst_id}] Unknown connection_type: {self.conn_type}")
                elif self.etl_step == "T":
                        self._transform_mssql(logging_step) #need logging_step to order parallel processing log entries
                elif self.etl_step == "L":
                        self._load_mssql(logging_step)      #need logging_step to order parallel processing log entries

        except Exception as e:
            print(f"[{self.job_inst_id}] Extract task [{self.job_task_name}] failed: {e}")
            log_error(self.job_inst_id, "extract", str(e), "JobTask.process()", logging_step+ 1)
            raise

        log_job_task(self.job_inst_task_id, "succeeded")
        print(f"{self.job_task_name} task succeeded")

        print(f"Finished task || {self.job_task_name}")
        log_info(job_inst_id=self.job_inst_id,
                 task_name=self.job_task_name,
                 info_message=f"Finished task || {self.job_task_name} ",
                 context="JobTask.process()",
                 logging_seq = logging_step + 19)
        return None

    def _kafka_produce_messages(self):

        print(f"[{self.job_inst_id}] reached _kafka_produce_messages")

        producer = KafkaProducerClient(
            topic=self.topic,
            msg_count= self.kafka_batch_record_size, # Number of records to buffer
            bootstrap_servers=self.bootstrap,
            retries=3,
            row =self.row  #job task data for db logging
        )

        producer.generate_fake_data()

    def _extract_kafka_data(self):

        try:

            consumer = KafkaETLConsumer(
                topic=self.topic,
                topic_pattern=self.topic_pattern,
                bootstrap_servers=self.bootstrap,
                group_id=self.group_id,
                sql_table=self.target_table,
                dlq_topic="dead-letter-que",
                batch_size=self.kafka_batch_record_size, # Number of records to buffer before writing to the database
                row = self.row  #job task data for db logging

            )

            consumer.run(self.max_messages)  # Set limit if we don’t want to consume forever

        except UnknownTopicOrPartitionError:
            print(f"Error: The topic || {self.topic} || does not exist. Check Kafka settings.")
        except Exception as e:
            print(f"An unexpected error or exception occurred: {e}")
            raise Exception(f"Kafka consumer failed: {e}")

    def _extract_mssql(self, _logging_step  = 1):
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
                 , context=f"{inspect.currentframe().f_code.co_name}"
                 , logging_seq=_logging_step + 2
                 )

        print(f"target || {self.engine_tgt}")

        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        print(f"SQL Statement || {self.sql_text}")
        log_info(job_inst_id=self.job_inst_id
                 , task_name=self.job_task_name
                 , info_message=f"SQL Statement || {self.sql_text}"
                 , context=f"{inspect.currentframe().f_code.co_name}"
                 , logging_seq=_logging_step + 3
                 )
        # if stored procedure (STAGE database):
        if self.sql_type.lower() == "proc":

            # a SQLAlchemy-compatible URL: "mssql+pyodbc://uname:pass@ip_address/db_name?driver=ODBC+Driver+17+for+SQL+Server"
            # (Replaced spaces in the driver name with + )
            _engine_src = self.engine_tgt  # Stage db

            print(f"source || {_engine_src}")
            log_info(job_inst_id=self.job_inst_id
                     , task_name=self.job_task_name
                     , info_message=f"source || {_engine_src}"
                     , context=f"{inspect.currentframe().f_code.co_name}"
                     , logging_seq=_logging_step + 4
                     )

            try:
                with _engine_src.connect() as connection:
                    trans = connection.begin()
                    try:
                        _sql_text = f"""
                                          EXEC {self.sql_text}  
                                              @p_job_inst_id = :param1,
                                              @p_job_inst_task_id = :param2,
                                              @p_logging_seq = :param3
                                      """
                        connection.execute(
                            text(_sql_text),
                            {"param1": self.job_inst_id,
                                        "param2": self.job_inst_task_id, "param3": _logging_step + 5}
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
                          , context=f"{inspect.currentframe().f_code.co_name}"
                          , logging_seq=_logging_step + 4
                          )
                log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
                raise
        # not a stored proc:
        else:
            # panda style conn_str: 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=xxx;DATABASE=xxx;UID=xxx;PWD=xxx'
            # no +
            _engine_src = self._get_engine_for_MsSql()

            print(f"Source || {_engine_src}")
            log_info(job_inst_id=self.job_inst_id
                     , task_name=self.job_task_name
                     , info_message=f"source || {_engine_src}"
                     , context=f"{inspect.currentframe().f_code.co_name}"
                     , logging_seq=_logging_step + 5
                     )

            # add incremental date if it is not a full load:
            if not self.is_full_load:
                sql_text = f"{self.sql_text}  and {self.incr_column} >= '{self.incr_date}'"
            else:
                sql_text = self.sql_text

            try:
                with _engine_src.connect() as conn_src:

                    df = pd.read_sql(sql_text, con=conn_src.execution_options(stream_results=True))

                    if self.is_large:
                        # Write to CSV

                        filename = f"{self.target_table.split('.')[-1]}.csv" #testing
                        #filename = f"{filename.replace('.', '_')}_{timestamp}.csv"

                        # This is inside the container, and it's mapped to the Windows share
                        docker_shared_path = "/opt/airflow/tmp/"
                        file_path = os.path.join(docker_shared_path, filename)

                        # Write CSV to shared folder
                        # testing
                        if os.path.exists(file_path):
                            os.remove(file_path)
                        # testing
                        df.to_csv(file_path, index=False, sep='|', encoding='CP1252')

                        # The path as seen by SQL Server (UNC path)
                        sql_server_path = f"\\\\192.168.86.96\\shared\\bulk_files\\{filename}"

                        # Append a query to capture the row count after the bulk insert
                        # ROWTERMINATOR = '0x0A' ensures UNIX-style line endings \n are correctly parsed.
                        # CODEPAGE = 'ACP' handles ANSI encoding. Use '65001' if your file is UTF-8.
                        bulk_insert_sql = f"""
                               BULK INSERT {self.target_table}
                               FROM '{sql_server_path}'
                               WITH (
                                   FIRSTROW = 2,
                                   FIELDTERMINATOR = '|',
                                   ROWTERMINATOR = '0x0A',
                                   CODEPAGE = 'ACP', /* assumes ANSI / cp1252 */
                                   TABLOCK
                               );
                           """

                        print(bulk_insert_sql)
                        log_info(job_inst_id=self.job_inst_id
                                 , task_name=self.job_task_name
                                 , info_message=f"SQL Statement || {bulk_insert_sql}"
                                 , context=f"{inspect.currentframe().f_code.co_name}"
                                 , logging_seq=_logging_step + 6
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
                                     , context=f"{inspect.currentframe().f_code.co_name}"
                                     , logging_seq=_logging_step + 7
                                     )

                        # Delete temp file if needed
                        if self.del_temp_data and os.path.exists(file_path):
                            os.remove(file_path)

                            print(f"Dropped {file_path}")
                            log_info(job_inst_id=self.job_inst_id
                                     , task_name=self.job_task_name
                                     , info_message=f"Dropped {file_path}"
                                     , context=f"{inspect.currentframe().f_code.co_name}"
                                     , logging_seq=_logging_step + 8
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
                            df.head(0).to_sql(temp_table, con=conn_tgt, if_exists="replace", index=False)

                            # Insert data
                            df.to_sql(temp_table, con=conn_tgt, if_exists="append", index=False, method="multi")

                            # Move to target table
                            insert_sql = f"INSERT INTO {self.target_table} SELECT * FROM {temp_table}"

                            print(f"SQL Statement || {insert_sql}")
                            log_info(job_inst_id=self.job_inst_id
                                     , task_name=self.job_task_name
                                     , info_message=f"SQL Statement || {insert_sql}"
                                     , context=f"{inspect.currentframe().f_code.co_name}"
                                     , logging_seq=_logging_step + 9
                                     )

                            result = conn_tgt.execute(text(insert_sql))
                            rowcount = result.rowcount
                            print(f"Target table [{self.target_table}] || inserted {rowcount} rows")
                            log_info(job_inst_id=self.job_inst_id
                                     , task_name=self.job_task_name
                                     , info_message=f"Target table [{self.target_table}] || inserted {rowcount} rows"
                                     , context=f"{inspect.currentframe().f_code.co_name}"
                                     , logging_seq=_logging_step + 10
                                     )

                            # Drop temp table if requested
                            if self.del_temp_data:
                                try:
                                    conn_tgt.execute(text(f"DROP TABLE {temp_table}"))
                                    print(f"Dropped temp table || {temp_table}")
                                    log_info(job_inst_id=self.job_inst_id
                                             , task_name=self.job_task_name
                                             , info_message=f"Dropped temp table || {temp_table}"
                                             , context=f"{inspect.currentframe().f_code.co_name}"
                                             , logging_seq=_logging_step + 11
                                             )
                                except Exception as drop_err:
                                    print(f"Warning: Failed to drop temp table {temp_table}: {drop_err}")
                                    log_error(job_inst_id=self.job_inst_id
                                              , task_name=self.job_task_name
                                              , error_message=f"Failed to drop temp table {temp_table}: {drop_err}"
                                              , context=f"{inspect.currentframe().f_code.co_name}"
                                              , logging_seq=_logging_step + 12
                                              )
                                    log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table

                    return len(df)

            except Exception as e:
                # [metadata].[log_dtl] table:
                log_error(job_inst_id=self.job_inst_id
                          , task_name=self.job_task_name
                          , error_message=str(e)
                          , context=f"{inspect.currentframe().f_code.co_name}"
                          , logging_seq=_logging_step + 13
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
        try:
            result =  file_fetch_and_save(self.row)
        except Exception as e:
            print(f"Error occurred: {e}")
            raise Exception(f"Error occurred: {e}")
        return result


    def _parquet_file(self):
        df = pd.read_parquet(self.file_path)
        print(f"[{self.job_inst_id}] Parquet loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        return df.shape[0]



    def _transform_mssql(self,  _logging_step  = 1) :

        """
        Purpose:
            Process 'transform' step of a job instance task.
            - via stored proc

        Called from: transform()
        """
        log_job_task(self.job_inst_task_id, "running")  # [metadata].[job_inst_task] table

        log_info(job_inst_id=self.job_inst_id
                 , task_name=f"{self.job_task_name}"
                 , info_message=f"target || {self.engine_tgt}"
                 , context=f"{inspect.currentframe().f_code.co_name}"
                 , logging_seq=_logging_step + 2
                 )
        print(f"target || {self.engine_tgt}")

        try:
                with self.engine_tgt.connect() as connection:
                    trans = connection.begin()
                    try:

                        proc_name = self.sql_text

                        _sql_text = f"""
                                           EXEC {self.sql_text}  
                                               @p_job_inst_id = :param1,
                                               @p_job_inst_task_id = :param2,
                                               @p_logging_seq = :param3
                                       """
                        connection.execute(
                            text(_sql_text),
                            {"param1": self.job_inst_id,
                             "param2": self.job_inst_task_id, "param3": _logging_step + 3}
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
                      , task_name=f"{self.job_task_name}"
                      , error_message=str(e)
                      , context=f"{inspect.currentframe().f_code.co_name}"
                      , logging_seq=_logging_step + 2
                      )
            log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
            raise

    def _load_mssql(self,  _logging_step  = 1) :

        """
        Purpose:
            Process 'transform' step of a job instance task.
            - via stored proc

        Called from: load()
        """
        log_job_task(self.job_inst_task_id, "running")  # [metadata].[job_inst_task] table


        log_info(job_inst_id=self.job_inst_id
                 , task_name=f"{self.job_task_name}"
                 , info_message=f"target || {self.engine_tgt}"
                 , context=f"{inspect.currentframe().f_code.co_name}"
                 , logging_seq=_logging_step + 2
                 )
        print(f"target || {self.engine_tgt}")

        try:
                with self.engine_tgt.connect() as connection:
                    trans = connection.begin()
                    try:

                        proc_name = self.sql_text

                        _sql_text = f"""
                                           EXEC {self.sql_text}  
                                               @p_job_inst_id = :param1,
                                               @p_job_inst_task_id = :param2,
                                               @p_logging_seq = :param3
                                       """
                        connection.execute(
                            text(_sql_text),
                            {"param1": self.job_inst_id,
                             "param2": self.job_inst_task_id, "param3": _logging_step + 3}
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
                      , task_name=f"{self.job_task_name}"
                      , error_message=str(e)
                      , context=f"{inspect.currentframe().f_code.co_name}"
                      , logging_seq=_logging_step + 2
                      )
            log_job_task(self.job_inst_task_id, "failed")  # [metadata].[job_inst_task] table
            raise

    def _get_engine_for_MsSql(self) :
        # MS-SQL-Server data source sample connection
        conn = BaseHook.get_connection("DB-29907-kliondb2017-id")
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
                            f"SERVER={conn.host};" \
                            f"DATABASE={conn.schema};" \
                            f"UID={conn.login};" \
                            f"PWD={conn.password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        return create_engine(connection_url)