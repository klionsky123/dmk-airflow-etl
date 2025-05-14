from kafka import KafkaProducer
from kafka.errors import KafkaError, UnknownTopicOrPartitionError
from faker import Faker
import json
import inspect
from helper import log_error, log_info, log_job_task, get_engine_for_metadata
import time


msg_count =1000 # number or fake records to generate

"""
    * Generate realistic fake data using the Faker library.
        # https://faker.readthedocs.io/en/master/
    * It requires installing kafka-python and Faker (pip install kafka-python faker).
    * how to handle errors:
        https://gpttutorpro.com/how-to-handle-errors-and-exceptions-with-kafka-and-python/
"""
fake = Faker()

class KafkaProducerClient:
    def __init__(self, topic = "test-topic2", msg_count = 1000, bootstrap_servers='kafka:9092', retries=3, row=None):
        self.topic = topic
        self.msg_count = msg_count
        self.bootstrap_servers = bootstrap_servers
        self.retries = retries

        self.job_inst_id = int(row.get("job_inst_id", 0))
        self.job_inst_task_id = int(row.get("job_inst_task_id", 0))
        self.job_task_name = row.get("job_task_name", "").strip()

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                acks='all',  # Wait for all replicas to acknowledge,
                batch_size=16384,  # 16KB batch size
                linger_ms=5,  # Wait up to 5ms to fill batches
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            log_info(job_inst_id=self.job_inst_id
                     , task_name=f"{self.job_task_name}"
                     , info_message="KafkaProducer initialized successfully."
                     , context=f"{inspect.currentframe().f_code.co_name}"
                     )
            print("KafkaProducer initialized successfully.")
        except KafkaError as e:
            log_error(job_inst_id=self.job_inst_id
                      , task_name=f"{self.job_task_name}"
                      , error_message=str(e)
                      , context=f"{inspect.currentframe().f_code.co_name}"
                      )
            print(f"Failed to initialize Kafka producer: {e}")
            raise

    def _send_message(self, topic, message):
        for attempt in range(self.retries):
            try:
                self.producer.send(topic, value=message).get(timeout=10)
                return True
            except UnknownTopicOrPartitionError:
                log_info(job_inst_id=self.job_inst_id
                         , task_name=f"{self.job_task_name}"
                         , info_message=f"Attempt {attempt+1}: Topic '{topic}' not found, retrying..."
                         , context=f"{inspect.currentframe().f_code.co_name}"
                         )
                print(f"Attempt {attempt+1}: Topic '{topic}' not found, retrying...")
                time.sleep(5)
            except KafkaError as e:
                log_error(job_inst_id=self.job_inst_id
                          , task_name=f"{self.job_task_name}"
                          , error_message=str(e)
                          , context=f"{inspect.currentframe().f_code.co_name}"
                          )
                print(f"Kafka producer error: {e}")
                raise

        log_error(job_inst_id=self.job_inst_id
                  , task_name=f"{self.job_task_name}"
                  , error_message=f"Failed to send message after {self.retries} attempts"
                  , context=f"{inspect.currentframe().f_code.co_name}"
                  )
        print(f"Failed to send message after {self.retries} attempts")
        return False

    def _flush_and_close(self):
        try:
            self.producer.flush()
            log_info(job_inst_id=self.job_inst_id
                     , task_name=f"{self.job_task_name}"
                     , info_message="All messages flushed successfully."
                     , context=f"{inspect.currentframe().f_code.co_name}"
                     )
            print("All messages flushed successfully.")
        except KafkaError as e:
            log_error(job_inst_id=self.job_inst_id
                      , task_name=f"{self.job_task_name}"
                      , error_message=str(e)
                      , context=f"{inspect.currentframe().f_code.co_name}"
                      )
            print(f"Error while flushing messages: {e}")
        finally:
            self.producer.close()
            log_info(job_inst_id=self.job_inst_id
                     , task_name=f"{self.job_task_name}"
                     , info_message="Kafka producer connection closed."
                     , context=f"{inspect.currentframe().f_code.co_name}"
                     )
            print("Kafka producer connection closed.")

    def generate_fake_data(self):
        """
        Generate topic names dynamically (e.g., test-topic1, test-topic2, …).
        Create messages for each topic.
        Send messages to each topic using the KafkaProducer.
        """
        log_job_task(self.job_inst_task_id, "running")  # [metadata].[job_inst_task] table

        num_topics = 3
        for i in range(1, num_topics + 1):
            topic = f"{self.topic}{i}"  # e.g., test-topic1, test-topic2, ...

            _info_msg =f"Producing {msg_count} messages to topic: {topic}"
            print(_info_msg)
            log_info(job_inst_id=self.job_inst_id
                     , task_name=f"{self.job_task_name}"
                     , info_message=_info_msg
                     , context=f"{inspect.currentframe().f_code.co_name}"
                     )

            # Generate and send messages
            for _ in range(msg_count):
                message = {
                    "name": fake.name(),
                    "address": fake.address(),
                    "transaction_id": fake.uuid4(),
                    "amount": fake.random_number(digits=5)
                }
                self._send_message(topic, message)

        self._flush_and_close()

        # report success:
        log_job_task(self.job_inst_task_id, "succeeded")  # [metadata].[job_inst_task] table






