from datetime import datetime, timedelta
import pandas as pd
import operator
from IPython.display import clear_output
from pyspark.sql import SparkSession
from scipy import stats
import matplotlib.pyplot as plt
import numpy as np
import os
import math
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler 
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
# assign directory


def ctag(df: pd.core.frame.DataFrame, name_target: str):
        df[name_target] = np.nan

def get_files(directory:str) ->list:
    '''
       Input: directory -> str
       Output: list with path of files in directory
     '''
    v = []
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
    # checking if it is a file
        if os.path.isfile(f):
               v.append(f)
    return v



def typec(df: pd.core.frame.DataFrame):
    '''
        Input: df -> Pandas DataFrame
    Print type of all columns of df'''
    for x in df.columns:
        print(x,': ',type(df.loc[0,x]))

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def dropc(df: pd.core.frame.DataFrame, vcolumns):
    df.drop(columns = vcolumns, inplace=True)
    


def cloc(df: pd.core.frame.DataFrame ,eq: str, val) -> pd.core.frame.DataFrame:
    '''Input: df ->pd.core.frame.DataFrame, eq->str, val -> Depends
       Output: DataFrame like the following example:
                   Find dataframe where column spider have values bigger than 250:
                         cloc(df,"spider ==",250)
    '''
    eq = eq.split(" ")
    ops = {
    '+' : operator.add,
    '-' : operator.sub,
    '*' : operator.mul,
    '/' : operator.truediv, 
    '%' : operator.mod,
    '^' : operator.xor,
    '==': operator.eq,
    '!=': operator.ne,
    '<' : operator.lt,
    '<=': operator.le,
    '>' : operator.gt,
    '>=' : operator.ge,   
    }
    return df.loc[ops[eq[1]](df[eq[0]],val)]

def savecsv_spark(sparkDF,path: str,overwrite=True):
    if(overwrite):
        sparkDF.write.mode('overwrite').option("header", "true").csv(path)
    else:
        sparkDF.write.option("header", "true").csv(path)
        
        
        
def replace_comma(df,no_columns={}):
    for y in df.columns:
        if(not(y in no_columns)):
            for x in range(0,len(df)):
                try:
                    df[y][x] = df[y][x].replace(',','.')
                except:
                    print("not float")
            df[y] = pd.to_numeric(df[y])
    return df



def ri(df: pd.core.frame.DataFrame):
    '''
    Input: df ->pd.core.frame.DataFrame
    
    Reset index of a DataFrame df'''
    df.reset_index(inplace=True)
    try:
        df.drop(columns=["index"],inplace = True)
    except:
        pass



def read_df(path, remove_nan =  False,sub = [],deci ='.',drop=[], treat_time = False, nametime = ''):
        df =  pd.read_csv(path, decimal =  deci)
        if(remove_nan):
            if(sub == []):
                df.dropna(inplace=True)
            else:
                df.dropna(subset = sub, inplace=True)
        if(drop!=[]):
            df.drop(columns = drop, inplace=True)
        if(treat_time):
            tr_time(form, df =  dfaux, nametime = nametime)
        if(nametime != ''):
            df[nametime] = pd.to_datetime(df[nametime])
        try:
            df.drop(columns = ['Unnamed: 0'],inplace=True)
        except:
            pass
        return df

    
def printc(*argv):
    for args in argv:
        print(args.columns)
        
def match_times(df1, df2, x, timex, y, timey,lag_time,d,newc = '',norepeat =False):
    print('here')
    df2aux = cloc(df2,df2.columns[timey],'>=',df1.iat[0,timex])
    print(len(df2aux))
    be2 = df2aux.index[0]
    en2 = df2aux.index[-1]
    df1aux = cloc(df1,df1.columns[timex],'>=',df2.iat[0,timey])
    be1 =  df1aux.index[0]
    en1 = df1aux.index[-1]
    print(be2,en2)
    print(be1,en1)
    not_found = 0
    if(newc!=''):
        print(newc)
        df1[newc] = np.nan
   # print(be,end)
    for k in range(be2,en2+1):
        if(k%1000==0):
            print(k/len(df2))
        date = df2.iat[k,timey] - lag_time
        #print(date)
        #clear_output()
        i = be1
        j = en1 
        while(i<j-1):
                m =  int((i+j)/2)
                pdate = df1.iat[m,timex]
                if(pdate >= date):
                    j = m 
                else:
                    i=m
        #print(pdate)
        #print(i,j)
        rei = df1.iat[i,timex]
        rej = df1.iat[j,timex]
        deltai = abs((rei-date).total_seconds())
        deltaj = abs((rej-date).total_seconds())
        if(deltai < deltaj and deltai <= d.total_seconds()):
       #     print('get lower;','delta = ',abs((rei - date).total_seconds())) 
            if(norepeat):
                    if(df1[i,x]!=np.nan):
                        print('already used')
                    else:
                        df1.iat[i,x] = df2.iat[k,y]
            else:
                df1.iat[i,x] = df2.iat[k,y]
        elif(deltaj < deltai and deltaj <= d.total_seconds()):
        #    print('get upper;','delta = ',abs((rej - date).total_seconds())) 
            if(norepeat):
                    if(df1[j,x]!=np.nan):
                        print('already used')
                    else:
                        df1.iat[j,x] = df2.iat[k,y]
            else:
                df1.iat[j,x] = df2.iat[k,y]
        elif(deltaj == deltai and deltaj <=d.total_seconds()):
       #     print('get lower;','delta = ',abs((rei - date).total_seconds())) 
            if(norepeat):
                    if(df1[i,x]!=np.nan):
                        print('already used')
                    else:
                        df1.iat[i,x] = df2.iat[k,y]
            else:
                df1.iat[i,x] = df2.iat[k,y]
        else:
            not_found = not_found +1
    print('not_found =',not_found)

def dfc(df,cols):
    dfr = pd.DataFrame()
    for x in cols:
       dfr[x] = df[x]
    return dfr


def treat(x,col):
    try:
        el = json.loads(x[0])['telemetry'][col]
        return el 
    except:
        return np.nan
import sys
    

    
def diff_metric(df,t1,t2,error):
    dfaux = pd.DataFrame()
    dfaux.dropna(inplace=True )
    dfaux[t1] = df[t1]
    dfaux[t2] = df[t2]
    dfaux=(dfaux-dfaux.min())*1000/(dfaux.max()-dfaux.min())
    dfaux = dfaux.diff()
    dfaux.replace(0,error,inplace=True)
    dfaux.dropna(inplace=True )
    ri(dfaux)
    # for x in dfaux.columns:
    #     dfaux[x] = dfaux[x].diff()
    result =  abs(dfaux[t1] - dfaux[t2])**2
    return result.sum()/len(result)
 
def diff_metric_mult(df,t1,t2,error):
    dfaux = pd.DataFrame()
    dfaux.dropna(inplace=True )
    dfaux[t1] = df[t1]
    dfaux[t2] = df[t2]
    dfaux = dfaux.diff()
    dfaux.replace(0,error,inplace=True)
    dfaux.dropna(inplace=True)
    ri(dfaux)
    # for x in dfaux.columns:
    #     dfaux[x] = dfaux[x].diff()
    result =  dfaux[t1]*dfaux[t2]>0
    return result.sum()/len(result)

def diff_metric_div(df,t1,t2):
    dfaux = pd.DataFrame()
    dfaux = df.copy()
    dfaux= (dfaux-dfaux.min())*1000/(dfaux.max()-dfaux.min())
    dfaux = dfaux*1000
    dfaux.dropna(inplace=True)
    ri(dfaux)
    # for x in dfaux.columns:
    #     dfaux[x] = dfaux[x].diff()
    result =  abs(dfaux[t1]/(dfaux[t2]+1))
    return result.sum()/len(result)
