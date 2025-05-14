from kafka import KafkaConsumer
from marshmallow.utils import timestamp
from sqlalchemy import create_engine
import pandas as pd
import json
from kafka.errors import KafkaError, UnknownTopicOrPartitionError
from helper import log_error, log_info, log_job_task, get_engine_for_metadata, parse_table_name
import inspect
import uuid
from datetime import datetime
from collections import defaultdict # provides a default value for missing keys. Instead of raising a KeyError...

class KafkaETLConsumer:
    """
    This a custom Kafka consumer moving a Kafka topic to a SQL table.
    This s a streaming ingestion from Kafka into a SQL system, in batches.

    Serialization is the process of converting the state of an object into a form that can be persisted or transported.
    The complement of serialization is deserialization, which converts a stream into an object.
    Together, these processes allow data to be stored and transferred

    """
    def __init__(self, topic: str, topic_pattern: str, bootstrap_servers: str, group_id, sql_table, batch_size=100, row=None):
        self.topics = topic                          # Kafka topics to consume messages from
        self.topic_pattern = topic_pattern
        self.bootstrap_servers = bootstrap_servers  # List of Kafka brokers (e.g., localhost:9092).

        # Use a unique group ID to ensure offsets are not reused
        # and every run starts fresh from the beginning:
        self.group_id = f'airflow-consumer-{uuid.uuid4()}'
        self.sql_table = sql_table
        self.batch_size = batch_size                # Number of records to buffer before writing to the database
        self.buffer = []                            # A temporary list holding messages before flushing to the DB

        self.job_inst_id = int(row.get("job_inst_id", 0))
        self.etl_step = row.get("etl_step", "E").strip()
        self.job_inst_task_id = int(row.get("job_inst_task_id", 0))
        self.job_task_name = row.get("job_task_name", "").strip()

        self.consumer = KafkaConsumer(
            self.topics,
            bootstrap_servers=self.bootstrap_servers,
            # Start consuming messages from the beginning of the topic (the oldest available messages).
            auto_offset_reset='earliest',
            enable_auto_commit=False,           # Don't auto-commit to avoid skipping on retries
            group_id= self.group_id,            # Consumer group ID for Kafka offset tracking
            consumer_timeout_ms=15000,          # Stop after 5s if no new messages
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserialize message values from JSON
        )

        self.engine = get_engine_for_metadata() # create_engine(self.db_conn_str)

        # If topics is a pattern string, subscribe to topics matching the regex pattern:
        if isinstance(self.topics, str):
             self.consumer.subscribe(pattern=self.topic_pattern) # pattern=r'test-.*'

        _info_msg =(f"KafkaConsumer initialized successfully; "
                    f"topic_pattern| {self.topic_pattern} | bootstrap_servers| {self.bootstrap_servers}")

        log_info(job_inst_id=self.job_inst_id
                 , task_name=f"{self.job_task_name}"
                 , info_message=_info_msg
                 , context=f"{inspect.currentframe().f_code.co_name}"
                 )
        print(_info_msg)

    def _enhance_message(self, message:dict, group_id:str, topic: str):
        # add group_id & timestamp, topic to the message:
        _timestamp = datetime.now()
        message.update({"group_id": group_id, "date_created": _timestamp, "kafka_topic": topic})

        return message

    def _flush_to_db(self, topic:str = None):
        """
        Flushes buffered messages to the SQL table:
                Converts self.buffer into a pandas DataFrame.
                Writes the data to the database using to_sql():
                Clears the buffer after the write.
        """
        if not self.buffer:
            return

        try:
            df = pd.DataFrame(self.buffer)

            # parse fully qualified table name
            tbl_schema, table = parse_table_name(self.sql_table)
            # schema = 'raw', table = 'my_table'

            # Ensure the table exists with the correct schema
            # df.head(0) gives the column structure without rows.
            df.head(0).to_sql(name=table, schema=tbl_schema, con=self.engine, if_exists="append", index=False)

            # Insert data; Avoid method="multi" with SQL Server via pyodbc, it produces errors
            df.to_sql(name=table, schema=tbl_schema, con=self.engine, if_exists="append", index=False)
            
            if topic:
                info_msg = f"Topic | {topic} | Flushed {len(self.buffer)} records to | {self.sql_table} "
            else:
                info_msg = f"Flushed {len(self.buffer)} records to | {self.sql_table} "
                
            print(info_msg)
            log_info(job_inst_id=self.job_inst_id
                     , task_name=f"{self.job_task_name}"
                     , info_message= info_msg
                     , context=f"{inspect.currentframe().f_code.co_name}"
                     )

            self.buffer.clear()

        except KafkaError as e:
            log_error(job_inst_id=self.job_inst_id
                      , task_name=f"{self.job_task_name}"
                      , error_message=str(e)
                      , context=f"{inspect.currentframe().f_code.co_name}"
                      )

    def run(self, max_messages=None):
        """
        Starts the Kafka consumer loop:
            Consumes messages from Kafka.
            for each topic: 
            Processes and appends each message to self.buffer.
            Flushes to the DB when buffer reaches batch_size.
            stop all processing once each topic reaches max_messages
        """
        log_job_task(self.job_inst_task_id, "running")  # [metadata].[job_inst_task] table

        print(f"Starting Kafka consumer group_id | {self.group_id} | and  max_messages per topic | {max_messages}")
        log_info(job_inst_id=self.job_inst_id
                 , task_name=f"{self.job_task_name}"
                 , info_message=f"Starting Kafka consumer group_id | {self.group_id} | and  max_messages per topic | {max_messages}"
                 , context=f"{inspect.currentframe().f_code.co_name}"
                 )
          
        counts = defaultdict(int)  # Track message count per topic
        completed_topics = set()  # Topics that reached max_messages
        # topics that we subscribed to:
        # subscribed_topics = set(self.consumer.subscription())  # this does not work - always empty; 

        # Force initial poll to populate assignment()
        self.consumer.poll(timeout_ms=1000)

        # Extract actual assigned topics
        #  self.consumer.assignment() returns TopicPartition objects that are assigned after polling
        assigned_topics = set(tp.topic for tp in self.consumer.assignment())
        print("Assigned topics:", assigned_topics)

        # loop one-by one through all messages in consumer:
        try:
            for msg in self.consumer:
                topic = msg.topic

                 # Skip topics already completed
                if topic in completed_topics:
                    continue

                record = self._enhance_message(msg.value, self.group_id, topic)
                self.buffer.append(record)
                counts[topic] += 1  # Track message count per topic

                if len(self.buffer) >= self.batch_size:
                    self._flush_to_db(topic)


                # Mark topic as completed if it reached the limit
                if max_messages and counts[topic] >= max_messages:
                    completed_topics.add(topic)
                    print(f"Reached max_messages for topic: {topic}")
                    log_info(job_inst_id=self.job_inst_id
                             , task_name=f"{self.job_task_name}"
                             , info_message=f"Reached max_messages for topic: {topic}"
                             , context=f"{inspect.currentframe().f_code.co_name}"
                             )

                # Check if all topics are done
                if completed_topics == assigned_topics:
                    print("Reached max_messages for all topics. Exiting loop.")
                    log_info(job_inst_id=self.job_inst_id
                             , task_name=f"{self.job_task_name}"
                             , info_message="Reached max_messages for all topics. Exiting loop."
                             , context=f"{inspect.currentframe().f_code.co_name}"
                             )
                    break

            self._flush_to_db()  # Final flush after loop
        finally:
            self.consumer.close()
            _info_msg = f"Kafka consumer closed. Message count: {dict(counts)}"
            print(_info_msg)
            log_info(job_inst_id=self.job_inst_id
                     , task_name=f"{self.job_task_name}"
                     , info_message=_info_msg
                     , context=f"{inspect.currentframe().f_code.co_name}"
                         )
        # report success:
        log_job_task(self.job_inst_task_id, "succeeded")  # [metadata].[job_inst_task] table
