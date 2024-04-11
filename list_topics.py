from confluent_kafka.admin import AdminClient
# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Initialize AdminClient
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

# Get list of topics
topics = admin_client.list_topics().topics

# Print list of topics
print("List of topics:")
for topic_name in topics:
    print(topic_name)
