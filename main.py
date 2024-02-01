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
import numpy as np

#window_size=350
sample_size=317 #got this from our analysis on historical data
time_sampling=30



class SimpleReservoirSampling:
    def __init__(self, max_size, random_seed=None):
        self.max_size = max_size
        self.samples = pd.DataFrame()
        self.random_seed = random_seed
        if self.random_seed is not None:
            random.seed(self.random_seed)

    def add_element(self, record):
        if len(self.samples) < self.max_size:
            self.samples = pd.concat([self.samples, record.to_frame().transpose()], ignore_index=True)
        elif random.random() < self.max_size / (len(self.samples) + 1):
            spot = random.randint(0, self.max_size - 1)
            self.samples.at[spot, 'time'] = record.index
            self.samples.at[spot, 'Speed'] = record['vmed']
            self.samples.at[spot, 'speed_lag_2'] = record['speed_lag_2']

# Set a random seed for reproducibility
random_seed = 42  




#topic name
topic='raw_traffic_data'





# Kafka producer configuration
# producer_config = {
#     'bootstrap.servers': 'localhost:9092',
#     'client.id': 'traffic_producer'
# }





# Initialize Kafka producer
# kafka_producer = Producer(producer_config)






# Custom JSON encoder to handle datetime objects

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)





# Function to send data to Kafka
# def send_to_kafka(data, topic):
#     kafka_producer.produce(topic, value=json.dumps(data, cls=DateTimeEncoder))
#     kafka_producer.flush()

#to read from URL
# def data_traffic_read():
#     # Create a pool manager
#     http = urllib3.PoolManager()

#     # Make an HTTP GET request
#     #print("Before HTTP request")
#     response = http.request('GET', 'http://informo.munimadrid.es/informo/tmadrid/pm.xml')
#     #print("After HTTP request")
#     # Check if the request was successful (status code 200)
#     print(response.status)
#     if response.status == 200:
#         #print("Hellooo")
#         xml_str = response.data
#         #print(xml_str)
#         root = ET.fromstring(xml_str)
#         data_list = []

#         #count= 0

#         for location in root.findall('pm'):
            
#             # Initialize codigo to a default value
#             codigo = None
#             intensity = 0.0
#             velocity = 0.0
#             occupancy = 0.0
#             #error = None

#             # Check if 'idelem' element exists
#             idelem_element = location.find('idelem')
#             if idelem_element is not None:
#                 # Find the 'codigo' element inside 'idelem'
#                 codigo_element = idelem_element.find('codigo')
#                 if codigo_element is not None:
#                     # Update 'codigo' with the text content of 'codigo_element'
#                     codigo = codigo_element.text.strip()
#                     #print('codigo:', codigo)
                
#                 intensity_element = location.find('intensidad')
#                 if intensity_element is not None:
#                     intensity = float(intensity_element.text.strip())
#                     #print('intensity:', intensity)
#                 #else:
#                     #print("intensidad element not found")

#                 velocity_element = location.find('velocidad')
#                 if velocity_element is not None:
#                     velocity = float(velocity_element.text.strip())
#                     #print('velocity:', velocity)
#                 #else:
#                     #print("velocidad element not found")

#                 occupancy = location.find('ocupacion')
#                 if occupancy is not None:
#                     ocupacion = float(occupancy.text.strip())
#                     #print('ocupacion:', ocupacion)
#                 #else:
#                     #print("ocupacion element not found")

#                 error = location.findtext('error', default='S').strip()
                

#                 if error == 'N':
#                     date = datetime.datetime.utcnow()
#                     message = {'ID': codigo, 'TrafficIntensity': intensity, 'TrafficSpeed': velocity,
#                                'TrafficOccupancy': ocupacion, 'Date_UTC': date}
#                     #print(message)
#                     #count=count+1
#                     data_list.append(message)

#                     # Send data to Kafka (assuming 'topic' is defined somewhere in your code)
#                     #send_to_kafka(message, topic)
#             #element_counter += 1
#             #if element_counter >= 20:
#                 #break
#         #print("count",count)
#         return data_list
#     else:
#         print("Failed to retrieve data. HTTP Status Code:", response.status)
#         return []
    


def data_traffic_read():
    # Create a pool manager
    http = urllib3.PoolManager()

    # Make an HTTP GET request
    response = http.request('GET', 'http://informo.munimadrid.es/informo/tmadrid/pm.xml')
    
    # Check if the request was successful (status code 200)
    if response.status == 200:
        xml_str = response.data
        root = ET.fromstring(xml_str)

        # Iterate over data records and yield each record
        for location in root.findall('pm'):
            codigo = None
            intensity = 0.0
            velocity = 0.0
            occupancy = 0.0
            error = 'S'

            idelem_element = location.find('idelem')
            if idelem_element is not None:
                codigo_element = idelem_element.find('codigo')
                if codigo_element is not None:
                    codigo = codigo_element.text.strip()
                
                intensity_element = location.find('intensidad')
                if intensity_element is not None:
                    intensity = float(intensity_element.text.strip())

                velocity_element = location.find('velocidad')
                if velocity_element is not None:
                    velocity = float(velocity_element.text.strip())

                occupancy_element = location.find('ocupacion')
                if occupancy_element is not None:
                    occupancy = float(occupancy_element.text.strip())

                error = location.findtext('error', default='S').strip()

            if error == 'N':
                date = datetime.datetime.utcnow()
                message = {'ID': codigo, 'TrafficIntensity': intensity, 'TrafficSpeed': velocity,
                           'TrafficOccupancy': occupancy, 'Date_UTC': date}
                yield message
    else:
        print("Failed to retrieve data. HTTP Status Code:", response.status)
    


















#To make predictions
# def pred(df):
#     X = []
#     Y = []
#     for index, row in df.iterrows():
#         X.append([index.hour, index.minute])
#         Y.append(row)
        

#     #initializes the model
#     svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.1)


#     y_rbf = svr_rbf.fit(X, Y.values.ravel())

#     #predicting for next readings

#     #first we take the time for the latest reading
#     length = len(df)
#     df_pred = df[length-1:length]

#     for index, row in df_pred.iterrows():
#         time_last = index
    
#     print ("time_last is {}".format(time_last))
#     #extracting time for next 3 predictions

#     X_pred = []
#     for i in range(3):
#         time_new = time_last + datetime.timedelta(seconds = (i+1)*time_sampling)
#         X_pred.append([time_new.hour, time_new.minute])

#     Y_pred = y_rbf.predict(X_pred)
    #return X_pred, Y_pred
        


# def pred(df):
#     X = []
#     Y = []

#     for index, row in df.iterrows():
#         X.append([index.hour, index.minute])
#         Y.append(row.tolist())  # Append the entire row as a list

#     # Initialize the SVR model
#     svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.1)

#     # Convert Y to a DataFrame for fitting
#     Y_df = pd.DataFrame(Y)

#     # Fit the SVR model
#     svr_rbf.fit(X, Y_df)

#     # Predicting for next readings
#     length = len(df)
#     df_pred = df[length - 1:length]

#     for index, row in df_pred.iterrows():
#         time_last = index
    
#     print("time_last is {}".format(time_last))
    
#     # Extracting time for next 3 predictions
#     X_pred = []
#     for i in range(3):
#         time_new = time_last + datetime.timedelta(seconds=(i + 1) * time_sampling)
#         X_pred.append([time_new.hour, time_new.minute])

#     Y_pred = svr_rbf.predict(X_pred)
#     return X_pred, Y_pred


def pred(df):
    X = []
    Y = []

    for index, row in df.iterrows():
        X.append([index.hour, index.minute])
        Y.append(row.tolist())  # Append the entire row as a list

    # Initialize the SVR model
    svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.1)

    # Convert Y to a 1-dimensional array
    Y = np.array(Y).ravel()

    # Fit the SVR model
    svr_rbf.fit(X, Y)

    # Predicting for next readings
    length = len(df)
    df_pred = df[length - 1:length]

    for index, row in df_pred.iterrows():
        time_last = index
    
    print("time_last is {}".format(time_last))
    
    # Extracting time for next 3 predictions
    X_pred = []
    for i in range(3):
        time_new = time_last + datetime.timedelta(seconds=(i + 1) * time_sampling)
        X_pred.append([time_new.hour, time_new.minute])

    Y_pred = svr_rbf.predict(X_pred)
    return X_pred, Y_pred
    





#adaptive movie window regression function
# def AMWR(df):
#     #extracting last readings equivalent to sample size

#     length = len(df)
#     df1 = df[length-sample_size:length]


#     df_speed = df1[['Date_UTC','TrafficSpeed']]
#     df_speed1 = df_speed.set_index('Date_UTC')
#     X_speed, Y_speed = pred(df_speed1)

#     df_intensity = df1[['Date_UTC','TrafficIntensity']]
#     df_intensity1 = df_intensity.set_index('Date_UTC')
#     X_intensity, Y_intensity = pred(df_intensity1)


#     for i in range(3):
#         print ("Expected traffic speed at {}:{} is {}".format(X_speed[i][0], X_speed[i][1], int(Y_speed[i])))
#         print ("Expected traffic intensity at {}:{} is {}".format(X_intensity[i][0], X_intensity[i][1], int(Y_intensity[i])))


def AMWR(df):
    # Extracting last readings equivalent to sample size
    length = len(df)
    df1 = df[length - sample_size:length]
    #print("Hello!")
    #print(df1)
    

    df_speed = df1[['Date_UTC', 'TrafficSpeed']]
    df_speed1 = df_speed.set_index('Date_UTC')
    # print("df_speed")
    # print(df_speed)
    X_speed, Y_speed = pred(df_speed1)

    # Ensure 'TrafficIntensity' is present in the DataFrame columns
    if 'TrafficIntensity' not in df1.columns:
        print("Error: 'TrafficIntensity' column not found in the DataFrame.")
        return

    df_intensity = df1[['Date_UTC', 'TrafficIntensity']]
    df_intensity1 = df_intensity.set_index('Date_UTC')
    # print("Intensity")
    # print(df_intensity1)
    X_intensity, Y_intensity = pred(df_intensity1)

    for i in range(3):
        print("Expected traffic speed at {}:{} is {}".format(X_speed[i][0], X_speed[i][1], int(Y_speed[i])))
        print("Expected traffic intensity at {}:{} is {}".format(X_intensity[i][0], X_intensity[i][1], int(Y_intensity[i])))



 #main function
# if __name__ == '__main__':

    
#     while(1):

#         list = data_traffic_read()
#         total_list = total_list + list
#         df = pd.DataFrame(total_list)
#         print (df)
#         df1 = df[df['ID'] == 'PM10344']
#         print (len(df1))

#         if len(df1) >= sample_size:
#             print ("calling prediction algo")
#             print ("df1 is ", df1)
#             AMWR(df1)

#         else:
#             pass

#         print ("i am sleeping")
#         time.sleep(time_sampling)
        
# Main function


# if __name__ == '__main__':
#     # Initialize an empty list to accumulate data for each sample
#     sample_data = []

#     # Create a generator object
#     data_generator = data_traffic_read()

#     # Loop to read data sample-wise until the generator is exhausted
#     while True:
#         # Read data record by record until the sample size is reached
#         for _ in range(sample_size):
#             try:
#                 data_record = next(data_generator)
#                 sample_data.append(data_record)
#             except StopIteration:
#                 break  # Break the inner loop if no more data is available from the generator

#         # If the length of the sample data equals the sample size, perform analysis
#         if len(sample_data) == sample_size:
#             print("Performing analysis for sample...")
#             print(len(sample_data))
#             # Convert sample_data to DataFrame
#             df = pd.DataFrame(sample_data)
#             # Perform analysis using AMWR
#             print("df")
#             print(df)
#             AMWR(df)

#             # Clear the sample data to start accumulating data for the next sample
#             sample_data = []

#         # If no more data is available, break the outer loop
#         if len(sample_data) < sample_size:
#             print("End of data reached.")
#             break




if __name__ == '__main__':
    # Initialize an empty list to accumulate data for each sample
    sample_data = []
    sample_number = 1  # Initialize sample number

    # Create a generator object
    data_generator = data_traffic_read()
    

    # Loop to read data sample-wise until the generator is exhausted
    while True:
        # Read data record by record until the sample size is reached
        for _ in range(sample_size):
            try:
                data_record = next(data_generator)
                sample_data.append(data_record)
            except StopIteration:
                break  # Break the inner loop if no more data is available from the generator
        else:
            # If the length of the sample data equals the sample size, perform analysis
            if len(sample_data) == sample_size:
                print(f"Performing analysis for sample {sample_number}...")
                #print(len(sample_data))
                # Convert sample_data to DataFrame
                df = pd.DataFrame(sample_data)
                # Perform analysis using AMWR
                # print("df")
                print(df)
                AMWR(df)

                # Clear the sample data to start accumulating data for the next sample
                sample_data = []
                sample_number += 1  # Increment sample number
                continue  # Continue to the next iteration of the while loop

        # If no more data is available, break the outer loop
        print("End of data reached.")
        break



