import matplotlib.pyplot as plt
import numpy as np
import math
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler 
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import sys
sys.path.append("/Workspace/Repos/gabrielcaiaffa@brainiall.com/cleanfunctions/func/")
from utility_functions import * 
from plot_functions import *


def ytest_ypred(model):
    X_test = model[2]
    Y_test = model[3]
    v=[[],[]]    
    y_pred = model[4].predict(X_test)
    for x in Y_test:
        v[0].append(x)
    for x in y_pred:
        v[1].append(x)
    return v


def normadf(df):
    df = (df - df.min())/(df.max()-df.min())
    return df 

def invnormadf(df_norma, df_original,cols = []):
    if cols == []:
        ma = df_original.max()
        mi = df_original.min()
        return df_norma(ma-mi)+mi
    else:
        aux_df = pd.DataFrame()
        for x in cols:
             ma = df_original[x].max()
             mi = df_original[x].min()
             aux_df[x] = df_norma(ma-mi)+mi
        return aux_df
def get_splited(df,sep,features,target, shuff = False):
    '''Split data in train and test: 
        sep = Percentage that will be used to train
        shuff = if True shuffle data'''
    dfaux = df.copy()
    if(shuff):
        dfaux  = dfaux.sample(frac=1).reset_index(drop=True)
    val_sep = int(dfaux.shape[0]*sep)
    x = df[features]
    y = df[target]
    x_train = x.iloc[:val_sep]
    x_test = x.iloc[val_sep:]
    y_train = y.iloc[:val_sep]
    y_test = y.iloc[val_sep:]
    return x_train,y_train,x_test,y_test  

def apply_model(df, features,target,model,sep=0.8,shuff = False, epochs=1, train = True):
    k = get_splited(df,sep=sep,target=target, features = features)
    X_train = k[0].to_numpy().reshape(-1,1,k[0].shape[1])
    y_train = k[1].to_numpy().reshape(-1,1,k[1].shape[1])
    X_test = k[2].to_numpy().reshape(-1,1,k[2].shape[1])
    y_test = k[3].to_numpy().reshape(-1,1,k[3].shape[1])
    if train == True:
        model.fit(X_train, y_train,use_multiprocessing = True, epochs=epochs)
    return X_train, y_train, X_test, y_test, model


def generate_model_simulate(df,features,target,mas,shift, train = True):
    ma_sensors = []
    data  = dfc(df,features+target)
    data .dropna(inplace=True)
    ri(data)
    for col in target:
        for val in mas:
            name = col+'_MA'+f"{val}"
            data [name] = data [col].rolling(window=val).mean()
            ma_sensors.append(name)
    for x in target+ma_sensors:
        data [x] = data [x].shift(periods= -shift)
    #data ['4203_WIT_001_t_h'] = data ['4203_WIT_001_t_h'].shift(periods=-G13)
    data.dropna(inplace = True)
    data_normalized= normadf(data)
    print('here')
    aux = data_normalized.to_numpy().reshape(-1,1,data_normalized.shape[1])
    model = keras.Sequential()
    model.add(layers.LSTM(128, return_sequences=True, input_shape=(1,aux.shape[2]-len(target))))
    model.add(layers.LSTM(256, return_sequences=False))
    model.add(layers.Dense(25))
    model.add(layers.Dense(len(target)))
    model.compile(loss='mse', optimizer='adam', metrics=['mae','mse'])
#     return data_normalized
    m = apply_model(df=data_normalized, epochs=1,model=model,sep=0.99,shuff=False,features= features+ma_sensors, target =target,train = train)
    return m