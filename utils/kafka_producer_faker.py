from kafka import KafkaProducer
from kafka.errors import KafkaError, UnknownTopicOrPartitionError
from faker import Faker
import json
import logging
import time

# Set up logging to the screen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

msg_count =1000 # number or fake records to generate

"""
    * Generate realistic fake data using the Faker library.
        # https://faker.readthedocs.io/en/master/
    * It requires installing kafka-python and Faker (pip install kafka-python faker).
    * how to handle errors:
        https://gpttutorpro.com/how-to-handle-errors-and-exceptions-with-kafka-and-python/
"""
fake = Faker()

try:
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        acks='all',  # Wait for all replicas to acknowledge,
        batch_size=16384,  # 16KB batch size
        linger_ms=5,  # Wait up to 5ms to fill batches
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except KafkaError as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    raise

# Function to send message with retry handling
def send_message(topic, message, retries=3):
    # send() Asynchronous, adds messages to the internal buffer.
    # We batch messages for efficiency, meaning they may not be sent instantly.
    for attempt in range(retries):
        try:
            producer.send(topic, value=message).get(timeout=10)
            # logger.info(f"Message sent successfully: {message}")
            return
        except UnknownTopicOrPartitionError:
            logger.warning(f"Attempt {attempt+1}: Topic '{topic}' not found, retrying...")
            time.sleep(5)  # Wait before retrying
        except KafkaError as e:
            logger.error(f"Kafka producer error: {e}")
            raise

    logger.error(f"Failed to send message after {retries} attempts")

# Generate and send messages
for _ in range(msg_count):
    message = {
        "name": fake.name(),
        "address": fake.address(),
        "transaction_id": fake.uuid4(),
        "amount": fake.random_number(digits=5)
    }
    send_message("test-topic", message)

logger.info(f"Sent {msg_count} messages successfully!")

# Ensure all messages are sent before closing producer
# flush() forces all buffered messages to be sent and waits for their completion.
# It ensures that all previously sent records are acknowledged before proceeding.
try:
    producer.flush()
    logger.info("All messages flushed successfully!")
except KafkaError as e:
    logger.error(f"Error while flushing messages: {e}")

producer.close()




