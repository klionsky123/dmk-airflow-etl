from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

def create_kafka_topic(topic_name, bootstrap_servers='kafka:9092', num_partitions=1, replication_factor=1):
    """
    Create `Dead Letter Que` Kafka topic
    Run in Docker, as we use kafka:9092 and not localhost:9092
    """
    for _ in range(10):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics([topic])
            print(f"Topic '{topic_name}' created.")
            return
        except TopicAlreadyExistsError:
            print(f"Topic '{topic_name}' already exists.")
            return
        except Exception as e:
            print(f"Kafka not ready, retrying... ({e})")
            time.sleep(5)

# 1. create topic_name="dead-letter-que"
create_kafka_topic(topic_name="dead-letter-que", bootstrap_servers="kafka:9092")

# create topic_name="test-topic-bad" to store 'bad' messages.
# once created, manually copy/create malformed messages to this topic.
# Run kafka consumer and it will process this topic. Its messages will end up in 'dead-letter-que' topic.
create_kafka_topic(topic_name="test-topic-bad", bootstrap_servers="kafka:9092")

if __name__ == "__main__":
    create_kafka_topic(topic_name="dead-letter-que", bootstrap_servers="kafka:9092")
    create_kafka_topic(topic_name="test-topic-bad", bootstrap_servers="kafka:9092")