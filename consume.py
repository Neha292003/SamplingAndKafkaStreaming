from collections import deque
from confluent_kafka import Consumer, TopicPartition
import csv
import json
import sys

bootstrap_servers = 'localhost:9092'
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest',  # Start consuming from the earliest offset
}

topic = 'raw_traffic_data'
consumer = Consumer(**consumer_config)

# Assign the consumer to the desired partition
partition = 0  # Assuming only one partition
consumer.assign([TopicPartition(topic, partition)])

# with open(sys.argv[1], 'r') as count_file:
#     count = int(count_file.read())
# print(count)
# Deque to store messages
message_buffer = deque(maxlen=1)

# CSV file path
csv_file_path = 'data_loc_10344.csv'

# Consume messages until no more messages are available
while True:
    #print("Entered while")
    message = consumer.poll(timeout=1.0)
    if message is None:
        print("No more messages available.")
        break  # No more messages
    elif message.error():
        print(f"Consumer error: {message.error()}")
        continue  # Skip this message
    
    # Parse message value as JSON
    message_value = json.loads(message.value())
    #print(message_value)
    # Append message value to the deque
    message_buffer.append(message_value)

# Write messages to the CSV file
with open(csv_file_path, 'a', newline='') as csvfile:
    fieldnames = ['ID', 'intensidad','TrafficOccupancy','carga','nivelServicio','intensidadSat','Date_UTC']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    for message in message_buffer:
        writer.writerow(message)

# Print confirmation
print("Messages appended to the CSV file:", csv_file_path)





# from collections import deque
# from confluent_kafka import Consumer, TopicPartition

# bootstrap_servers = 'localhost:9092'
# consumer_config = {
#     'bootstrap.servers': bootstrap_servers,
#     'group.id': 'my_consumer_group',
#     'auto.offset.reset': 'latest',  # Start consuming from the earliest offset
# }

# topic = 'raw_traffic_data'
# consumer = Consumer(**consumer_config)

# # Assign the consumer to the desired partition
# partition = 0  # Assuming only one partition
# consumer.assign([TopicPartition(topic, partition)])

# # Deque to store messages
# message_buffer = deque(maxlen=2)

# # Consume messages until no more messages are available
# while True:
#     message = consumer.poll(timeout=1.0)
#     if message is None:
#         print("No more messages available.")
#         break  # No more messages
#     elif message.error():
#         print(f"Consumer error: {message.error()}")
#         continue  # Skip this message
    
#     # Append message value to the deque
#     message_buffer.append(message.value())

# # Print the last 2 messages
# print("Last 2 messages:")
# for message in message_buffer:
#     print(message)
