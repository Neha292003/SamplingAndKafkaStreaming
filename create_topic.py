from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

# Kafka broker address
bootstrap_servers = 'localhost:9092'

def create_topic(bootstrap_servers, topic_name, num_partitions=1, replication_factor=1):
    # Initialize AdminClient
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Create NewTopic object
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)

    # Create topic
    try:
        admin_client.create_topics([new_topic])
        print("Topic '{}' created successfully.".format(topic_name))
    except KafkaException as e:
        print("Failed to create topic '{}': {}".format(topic_name, e))

if __name__ == "__main__":
    topic_name = "predictions_topic"  # Change this to your desired topic name
    create_topic(bootstrap_servers, topic_name)