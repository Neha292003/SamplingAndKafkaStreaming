import pandas as pd
from sklearn.svm import SVR
import datetime

sample_size=16
pred_window=3
time_sampling=320

import datetime
import pandas as pd
from sklearn.svm import SVR

def pred(df):
    X = []
    Y = []

    # Convert 'Date_UTC' column to datetime
    df.index = pd.to_datetime(df.index)

    # Sort DataFrame by index
    df.sort_index(inplace=True)

    # Extract hour and minute from index and prepare X and Y
    for idx, row in df.iterrows():
        X.append([idx.hour, idx.minute])
        Y.append(row['intensidad'])

    # Initialize the SVR model
    svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.1)

    # Train the SVR model
    svr_rbf.fit(X, Y)

    # Predicting for next readings

    # Extracting time for next 3 predictions
    time_last = df.index[-1]
    print("time_last")
    print(time_last)

    X_pred = []
    for i in range(3):
        time_new = time_last + datetime.timedelta(seconds=(i + 1) * time_sampling)
        X_pred.append([time_new.hour, time_new.minute])

    # Predicting the intensidad for next 3 timestamps
    Y_pred = svr_rbf.predict(X_pred)

    return X_pred, Y_pred

def AMWR(df):
    # Check if 'Date_UTC' column exists in DataFrame
    if 'Date_UTC' in df.columns:
        # Extracting last readings equivalent to window size
        #print("Hello")
        length = len(df)
        df1 = df[length - sample_size:length]

        # Prepare DataFrame for intensidad prediction
        df_intensidad = df1[['Date_UTC', 'intensidad']]
        df_intensidad1=df_intensidad.set_index('Date_UTC')

        # Predict intensidad for next three timestamps
        X_intensidad, Y_intensidad = pred(df_intensidad1)

        # Print predictions
        for i in range(3):
            print("Expected {} at {}:{} is {}".format('intensidad', X_intensidad[i][0], X_intensidad[i][1], int(Y_intensidad[i])))
    else:
        print("Error: 'Date_UTC' column not found in DataFrame.")


#main function
if __name__ == '__main__':
    df=pd.read_csv('final_data.csv')
    df['Date_UTC'] = pd.to_datetime(df['Date_UTC'])
    # print(df.head(5))
    df1 = df[df['ID'] == 10344]
    #print(df1.head(5))

    if len(df1) >= sample_size:
            #print ("calling prediction algo")
            AMWR(df1)






    
