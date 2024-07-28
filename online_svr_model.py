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
import matplotlib.pyplot as plt

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
            self.samples.at[spot, 'intensidad'] = record['intensidad']
            self.samples.at[spot, 'intensity_lag_2'] = record['intensity_lag_2']

# Set a random seed for reproducibility
random_seed = 42  

# Custom JSON encoder to handle datetime objects

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


# def data_traffic_read():
#     # Create a pool manager
#     http = urllib3.PoolManager()

#     # Make an HTTP GET request
#     response = http.request('GET', 'https://informo.madrid.es/informo/tmadrid/pm.xml')
    
#     # Check if the request was successful (status code 200)
#     if response.status == 200:
#         xml_str = response.data
#         root = ET.fromstring(xml_str)

#         # Iterate over data records and yield each record
#         for location in root.findall('pm'):
#             codigo = None
#             intensity = 0.0
#             velocity = 0.0
#             occupancy = 0.0
#             error = 'S'

#             idelem_element = location.find('idelem')
#             if idelem_element is not None:
#                 codigo_element = idelem_element.find('codigo')
#                 if codigo_element is not None:
#                     codigo = codigo_element.text.strip()
                
#                 intensity_element = location.find('intensidad')
#                 if intensity_element is not None:
#                     intensity = float(intensity_element.text.strip())

#                 velocity_element = location.find('velocidad')
#                 if velocity_element is not None:
#                     velocity = float(velocity_element.text.strip())

#                 occupancy_element = location.find('ocupacion')
#                 if occupancy_element is not None:
#                     occupancy = float(occupancy_element.text.strip())

#                 error = location.findtext('error', default='S').strip()

#             if error == 'N':
#                 date = datetime.datetime.utcnow()
#                 message = {'ID': codigo, 'TrafficIntensity': intensity, 'TrafficSpeed': velocity,
#                            'TrafficOccupancy': occupancy, 'Date_UTC': date}
#                 yield message
#     else:
#         print("Failed to retrieve data. HTTP Status Code:", response.status)
    

#reading data of a particular idelem(3409)
    
def data_traffic_read(idelem):
    # Create a pool manager
    http = urllib3.PoolManager()

    # Construct the URL with the provided idelem
    url = f'https://informo.madrid.es/informo/tmadrid/pm.xml?ide={idelem}'

    # Make an HTTP GET request
    response = http.request('GET', url)
    
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
    
    # Extracting time for next 10 predictions
    X_pred = []
    for i in range(3):
        time_new = time_last + datetime.timedelta(seconds=(i + 1) * time_sampling)
        X_pred.append([time_new.hour, time_new.minute])

    Y_pred = svr_rbf.predict(X_pred)
    return X_pred, Y_pred
    


def AMWR(df):
    # Extracting last readings equivalent to sample size
    length = len(df)
    df1 = df[length - sample_size:length]
    
    # Fetch real-time data for comparison
    real_time_speed = df1.iloc[-1]['TrafficSpeed']
    real_time_intensity = df1.iloc[-1]['TrafficIntensity']

    # Perform prediction
    df_speed = df1[['Date_UTC', 'TrafficSpeed']]
    df_speed1 = df_speed.set_index('Date_UTC')
    X_speed, Y_speed = pred(df_speed1)

    # Ensure 'TrafficIntensity' is present in the DataFrame columns
    if 'TrafficIntensity' not in df1.columns:
        print("Error: 'TrafficIntensity' column not found in the DataFrame.")
        return

    df_intensity = df1[['Date_UTC', 'TrafficIntensity']]
    df_intensity1 = df_intensity.set_index('Date_UTC')
    X_intensity, Y_intensity = pred(df_intensity1)

    for i in range(3):
        print("Expected traffic intensity at {}:{} is {}".format(X_intensity[i][0], X_intensity[i][1], int(Y_intensity[i])))

        # Calculate error between prediction and real-time data
        speed_error = abs(Y_speed[i] - real_time_speed)
        intensity_error = abs(Y_intensity[i] - real_time_intensity)

        print("Real-time traffic intensity:", real_time_intensity)
        print("Intensity prediction error:", intensity_error)

    return X_intensity, Y_intensity, real_time_intensity

# Accumulate data for plotting
all_predicted_intensity = []
all_real_time_intensity = []


import matplotlib.pyplot as plt

if __name__ == '__main__':
    # Accumulate data for plotting
    all_predicted_intensity = []
    all_real_time_intensity = []

    # Initialize an empty list to accumulate data for each sample
    sample_data = []
    sample_number = 1  # Initialize sample number

    # Create a generator object
    data_generator = data_traffic_read(idelem='3409')

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
                df = pd.DataFrame(sample_data)
                X_intensity, Y_intensity, real_time_intensity = AMWR(df)

                # Append intensities to accumulate data
                all_predicted_intensity.extend(Y_intensity)
                all_real_time_intensity.extend([real_time_intensity] * len(Y_intensity))

                # Plotting for the current sample
                plt.figure(figsize=(10, 5))
                plt.plot(Y_intensity, label='Predicted Intensity', marker='o', linestyle='-', color='red')
                plt.axhline(y=real_time_intensity, color='blue', linestyle='--', label='Real-time Intensity')
                plt.xlabel('Timestamp')
                plt.ylabel('Intensity')
                plt.title(f'Predicted vs Real-time Traffic Intensity for Sample {sample_number}')
                plt.legend()
                plt.grid(True)
                plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
                plt.tight_layout()  # Adjust layout to prevent overlapping labels
                plt.show()

                sample_number += 1  # Increment sample number
                sample_data = []  # Clear sample data for the next iteration
                continue

        # If no more data is available, break the outer loop
        print("End of data reached.")
        break

    # Plotting all intensities
    plt.figure(figsize=(10, 5))
    plt.plot(all_predicted_intensity, label='Predicted Intensity', marker='o', linestyle='-', color='red')
    plt.plot(all_real_time_intensity, label='Real-time Intensity', marker='o', linestyle='-', color='blue')
    plt.xlabel('Sample')
    plt.ylabel('Intensity')
    plt.title('Predicted vs Real-time Traffic Intensity for all Samples')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
