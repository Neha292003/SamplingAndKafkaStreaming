from confluent_kafka import Producer
import json
import urllib3
import xml.etree.ElementTree as ET
import re
import io, random
import pandas as pd
import time
from datetime import datetime
from sklearn.svm import SVR
import subprocess
import datetime
from datetime import timezone
import csv

# Kafka configuration
bootstrap_servers = 'localhost:9092'

producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'traffic_producer'
}

# Kafka topics
producer_topic = 'raw_traffic_data'

# Initialize Kafka producer and consumer
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

# Function to process traffic data

def process_traffic_data(count):
    
    #Create a pool manager
    http = urllib3.PoolManager()

    #Make an HTTP GET request
    response = http.request('GET', 'http://informo.munimadrid.es/informo/tmadrid/pm.xml')
    
    #Check if the request was successful (status code 200)
    print(response.status)
    
    if response.status == 200:
        
        xml_str = response.data
        
        root = ET.fromstring(xml_str)
        
        data_list = []

        for location in root.findall('pm'):
            
            # Initialize codigo to a default value
            codigo = None
            intensity = 0.0
            velocity = 0.0
            occupancy = 0.0
            #error = None
            
            # Check if 'idelem' element exists
            idelem_element = location.find('idelem')
            id=idelem_element.text.strip()
            
            if idelem_element is not None:
                
                # Find the 'codigo' element inside 'idelem'
                codigo_element = idelem_element.find('codigo')
                
                if codigo_element is not None:
                    # Update 'codigo' with the text content of 'codigo_element'
                    codigo = codigo_element.text.strip()
                    
                
                intensity_element = location.find('intensidad')
                if intensity_element is not None:
                    intensity = intensity_element.text.strip()
                
                occupancy = location.find('ocupacion')
                if occupancy is not None:
                    ocupacion = occupancy.text.strip()
                    

                carga_element = location.find('carga')
                if carga_element is not None:
                    carga = carga_element.text.strip()
                
                nivelServicio_element=location.find('nivelServicio')
                if nivelServicio_element is not None:
                    nivelServicio=nivelServicio_element.text

                intensidadSat_element=location.find('intensidadSat')
                if intensidadSat_element is not None:
                    intensidadSat=intensidadSat_element.text.strip()

                error = location.findtext('error', default='S').strip()
                

                if error == 'N':
                    if id=='10344':
                        date = datetime.datetime.now(timezone.utc)
                        message = {'ID':id , 'intensidad': intensity, 'TrafficOccupancy': ocupacion,'carga':carga,
                               'nivelServicio':nivelServicio,'intensidadSat':intensidadSat,'Date_UTC': date}
                        #print(message)
                        count=count+1
                        data_list.append(message)
        #print(count)
        # with open('count.txt', 'w') as count_file:
        #     count_file.write(str(count))
        for message in data_list:
            print(message)
            send_to_kafka(message, producer_topic)
            #print("consuming")
            #consume_and_append_to_csv()
        
        #subprocess.run(["python", "consume.py", "count.txt"])
        subprocess.run(["python", "consume.py"])
        # count=0
        # with open('count.txt', 'w') as count_file:
        #     count_file.write(str(count))
        time.sleep(320)
    else:
        print("Failed to retrieve data. HTTP Status Code:", response.status)
        return []

def main():
    count=0
    while True:
        print("Calling produce")
        process_traffic_data(count)

if __name__ == "__main__":
    main()

