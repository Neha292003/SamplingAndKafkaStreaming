from confluent_kafka import Consumer, KafkaError
import pandas as pd
import json
import datetime

#topic name
topic='raw_traffic_data'

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'traffic_consumer',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka consumer
kafka_consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
kafka_consumer.subscribe([topic])
#print("Fetching")

try:
    print("Entered try.")
    while True:
        #print("Entered while.")
        msg = kafka_consumer.poll(1000)

        if msg is None:
            print("No new messages. Continuing...")
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of partition.")
                continue
            else:
                print(msg.error())
                break
    
        print('Received message:', msg.value().decode('utf-8'))

except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    kafka_consumer.close()





