from sklearn.svm import SVR
import random
import pandas as pd
import numpy as np

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
random_seed = 42  # You can use any integer value as the seed



def  mape(y_test, y_pred):
    # Exclude entries where y_test is zero
    nonzero_mask = y_test != 0

    # Check if there are non-zero entries in y_test
    if np.any(nonzero_mask):
        y_test_nonzero = y_test[nonzero_mask]
        y_pred_nonzero = y_pred[nonzero_mask]

        # Calculate MAPE
        mape = np.mean(np.abs((y_test_nonzero - y_pred_nonzero) / y_test_nonzero)) * 100
        return mape


def amwr_svm(sample, window_size=350, initial_sample_size=320, prediction_window=96):
  threshold = 0.05
  sample_size = 320  #Initial reservoir size
  prediction_window =96
  window_size =350

  # Create an empty SVM model
  svm_model = SVR()

  # Initialize an empty window to store data
  window_data = pd.DataFrame()

  X_predicted = []
  Y_predicted = []

  iteration_mapes = []
  TrainingWindowArray = []
  previous_mape = 0

  # Iterate through the time series data
  for i in range(len(sample)):
      # Append the complete record to the window
      window_data = pd.concat([window_data, sample.iloc[i].to_frame().transpose()], ignore_index=True)

      if len(window_data) == window_size:
          # Perform reservoir sampling
          reservoir = SimpleReservoirSampling(sample_size,random_seed=random_seed)
          for index, row in window_data.iterrows():
              reservoir.add_element(row)

          sampled_records = reservoir.samples

          # Extract X and y to train the SVR model
          X = sampled_records[['vmed']].values
          y = sampled_records['speed_lag_2'].values

          # Split the data into train and test
          X_training = X[:-prediction_window]
          y_training = y[:-prediction_window]
          X_testing = X[-prediction_window:]
          y_testing = y[-prediction_window:]

          # Train the model and predict
          svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.00001)
          y_rbf = svr_rbf.fit(X_training, y_training).predict(X_testing)

          y_testing = y_testing.astype(float)
          y_rbf = y_rbf.astype(float)

          for j in y_rbf:
              Y_predicted.append(j)

          for k in X_testing:
              X_predicted.append(k)

          mape = mape(y_testing, y_rbf)

          iteration_mapes.append(mape)
          print("mape:", mape)  #mape more, less accurate, so increase the sample size, higher sample size means better representation
                                #mape less, more accurate, so decrease the sample size

          if mape > 20:
              if len(iteration_mapes) == 1:
                  sample_size = sample_size - 1
                  prediction_window = int(sample_size*30/100)
              elif mape > previous_mape:
                  sample_size =sample_size + 1
                  prediction_window = int(sample_size*30/100)
              elif mape <= previous_mape:
                  sample_size = sample_size - 1
                  prediction_window = int(sample_size*30/100)
              if sample_size < 5:
                  sample_size = 5
          elif mape < 15:
              sample_size = min(300, sample_size + 1)
              prediction_window = int(sample_size*30/100)

          TrainingWindowArray.append(sample_size)

          window_data = pd.DataFrame()
          previous_mape = mape

  # Print the modified TrainingWindowArray
  # print("Modified Sample Window Size for each iteration:")
#print(TrainingWindowArray)
  #print(iteration_mapes)

  return TrainingWindowArray, iteration_mapes

  
