from kafka import KafkaProducer
from faker import Faker
import json

"""
    * Generates realistic fake data using the Faker library.
        # https://faker.readthedocs.io/en/master/
    * Requires installing kafka-python and Faker (pip install kafka-python faker).
"""


fake = Faker()
producer = KafkaProducer(bootstrap_servers='kafka:9092',
                      #   'enable.idempotence': True,  # Ensures exactly-once semantics
                         acks= 'all',  # Wait for all replicas to acknowledge,
                         batch_size=16384,          # 16KB batch size
                         linger_ms=5,               # Wait up to 5ms to fill batches
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for _ in range(1000):
    message = {
        "name": fake.name(),
        "address": fake.address(),
        "transaction_id": fake.uuid4(),
        "amount": fake.random_number(digits=5)
    }

    # Send messages with explicit partitioning
    producer.send('test-topic',
                        #partition=hash(key) % 4,   # Custom partition strategy
                        value=message)
producer.flush()
