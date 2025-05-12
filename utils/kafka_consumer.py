from kafka import KafkaConsumer
from sqlalchemy import create_engine
import pandas as pd
import json

from tenacity import retry
from urllib3 import Retry

from helper import get_engine_for_metadata

class KafkaETLConsumer:
    """
    This a custom Kafka consumer moving a Kafka topic to a SQL table.
    This s a streaming ingestion from Kafka into a SQL system, in batches.

    Serialization is the process of converting the state of an object into a form that can be persisted or transported.
    The complement of serialization is deserialization, which converts a stream into an object.
    Together, these processes allow data to be stored and transferred

    """
    def __init__(self, topic, bootstrap_servers, group_id, sql_table, batch_size=100):
        self.topic = topic                          # Kafka topic to consume messages from
        self.bootstrap_servers = bootstrap_servers  # List of Kafka brokers (e.g., localhost:9092).
        self.group_id = group_id                    # Consumer group ID for Kafka offset tracking
        self.sql_table = sql_table
        self.batch_size = batch_size                # Number of records to buffer before writing to the database (default is 100)
        self.buffer = []                            # A temporary list holding messages before flushing to the DB

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=self.group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserialize message values from JSON
        )

        self.engine = get_engine_for_metadata() # create_engine(self.db_conn_str)

    def process_message(self, message):
        # Currently returns the raw message.
        # This can be customized for cleaning or transforming the message before saving.

        return message

    def flush_to_db(self):
        """
        Flushes buffered messages to the SQL table:
                Converts self.buffer into a pandas DataFrame.
                Writes the data to the database using to_sql():
                Clears the buffer after the write.
        """
        if not self.buffer:
            return

        df = pd.DataFrame(self.buffer)
        # df.head(0) gives the column structure without rows.
        df.head(0).to_sql(self.sql_table, self.engine, if_exists="replace", index=False)

        # Insert data
        df.to_sql(self.sql_table, self.engine, if_exists="append", index=False, method="multi")

        # df.to_sql(self.sql_table, self.engine, if_exists='replace', index=False)
        print(f"Flushed {len(self.buffer)} records to {self.sql_table}")
        self.buffer.clear()

    def run(self, max_messages=None):
        """
        Starts the Kafka consumer loop:
            Consumes messages from Kafka.
            Processes and appends each message to self.buffer.
            Flushes to the DB when buffer reaches batch_size.
            Stops after consuming max_messages (if given).
            On exit, ensures the final buffer flushes and closes the consumer.
        """
        count = 0
        try:
            for msg in self.consumer:
                record = self.process_message(msg.value)
                self.buffer.append(record)
                count += 1

                if len(self.buffer) >= self.batch_size:
                    self.flush_to_db()

                if max_messages and count >= max_messages:
                    break

            self.flush_to_db()  # Final flush
        finally:
            self.consumer.close()
            print("Kafka consumer closed.")
