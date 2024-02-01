from confluent_kafka import Producer
import json
import urllib3
import xml.etree.ElementTree as ET
import urllib
import re
import io, random
import pandas as pd
import time
from datetime import datetime
from sklearn.svm import SVR
import datetime

#topic name
topic='raw_traffic_data'

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'traffic_producer'
}

# Initialize Kafka producer
kafka_producer = Producer(producer_config)


# Custom JSON encoder to handle datetime objects

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

# Function to send data to Kafka
def send_to_kafka(data, topic):
    kafka_producer.produce(topic, value=json.dumps(data, cls=DateTimeEncoder))
    kafka_producer.flush()

# Example traffic data to be sent

'''traffic_data_to_send = {
    'location': 'Intersection A',
    'intensity': 120,
    'speed': 40,
    'timestamp': '2024-01-15T12:30:00'
}

send_to_kafka(traffic_data_to_send, topic)'''

def data_traffic_read():
    # Create a pool manager
    http = urllib3.PoolManager()

    # Make an HTTP GET request
    #print("Before HTTP request")
    response = http.request('GET', 'http://informo.munimadrid.es/informo/tmadrid/pm.xml')
    #print("After HTTP request")
    # Check if the request was successful (status code 200)
    print(response.status)
    if response.status == 200:
        #print("Hellooo")
        xml_str = response.data
        #print(xml_str)
        root = ET.fromstring(xml_str)
        data_list = []

        #count= 0

        for location in root.findall('pm'):
            
            # Initialize codigo to a default value
            codigo = None
            intensity = 0.0
            velocity = 0.0
            occupancy = 0.0
            #error = None

            # Check if 'idelem' element exists
            idelem_element = location.find('idelem')
            if idelem_element is not None:
                # Find the 'codigo' element inside 'idelem'
                codigo_element = idelem_element.find('codigo')
                if codigo_element is not None:
                    # Update 'codigo' with the text content of 'codigo_element'
                    codigo = codigo_element.text.strip()
                    #print('codigo:', codigo)
                
                intensity_element = location.find('intensidad')
                if intensity_element is not None:
                    intensity = float(intensity_element.text.strip())
                    #print('intensity:', intensity)
                #else:
                    #print("intensidad element not found")

                velocity_element = location.find('velocidad')
                if velocity_element is not None:
                    velocity = float(velocity_element.text.strip())
                    #print('velocity:', velocity)
                #else:
                    #print("velocidad element not found")

                occupancy = location.find('ocupacion')
                if occupancy is not None:
                    ocupacion = float(occupancy.text.strip())
                    #print('ocupacion:', ocupacion)
                #else:
                    #print("ocupacion element not found")

                error = location.findtext('error', default='S').strip()
                

                if error == 'N':
                    date = datetime.datetime.utcnow()
                    message = {'ID': codigo, 'TrafficIntensity': intensity, 'TrafficSpeed': velocity,
                               'TrafficOccupancy': ocupacion, 'Date_UTC': date}
                    #print(message)
                    #count=count+1
                    data_list.append(message)

                    # Send data to Kafka (assuming 'topic' is defined somewhere in your code)
                    send_to_kafka(message, topic)
            #element_counter += 1
            #if element_counter >= 20:
                #break
        #print("count",count)
        return data_list
    else:
        print("Failed to retrieve data. HTTP Status Code:", response.status)
        return []



if __name__ == '__main__':
    # Run the producer to read traffic data and send it to Kafka
    data_traffic_read()



