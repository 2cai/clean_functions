import math
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler 
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers


def plot_yt_ypred(test_pred,lim,subplots = 1, caption = [],rang =[]):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(1,subplots, figsize = (20,10))
    for x  in range(subplots):
        if rang == []:
            ax[x].plot(test_pred[x][0],'g*', label = "Test")
            ax[x].set_ylim(lim[0],lim[1])
            if len(caption) > 0:
                ax[x].set_xlabel(caption[x])
            ax[x].plot(test_pred[x][1], 'ro', label = "Predict")
            ax[x].legend()
        else:
            ax[x].set_ylim(lim[0],lim[1])
            ax[x].plot(test_pred[x][0][rang[0]:rang[1]],'g*', label = "Test")
            if len(caption) > 0:
                ax[x].set_xlabel(caption[x])
            ax[x].plot(test_pred[x][1][rang[0]:rang[1]], 'ro', label = "Predict")
            ax[x].legend()
            
    plt.show()


def bplot(df,x,y,be=-1,end=-1):
    '''Simple plot x vs y'''
    if(be!=-1 and end!=-1):
        xpoints = df[x][be:end]
        ypoints = df[y][be:end]
        
    else:
        xpoints = df[x]
        ypoints = df[y]
    plt.scatter(xpoints, ypoints)
    plt.show()