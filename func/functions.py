import pandas as pd
import pyspark.pandas as ps
import datetime as dt
from datetime import datetime
import operator
from IPython.display import clear_output
from pyspark.sql import SparkSession
import operator
import matplotlib.pyplot as plt
from scipy import stats
import os
# assign directory





def get_files(directory):
    v = []
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
    # checking if it is a file
        if os.path.isfile(f):
               v.append(f)
    return v



def typec(df):
    for x in df.columns:
        print(x,': ',type(df.loc[0,x]))

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def tr_time(s,form, nametime = ''):
    s = s.split(' ')
    ap =  s[-1]
    s = s[0]+' '+s[1]
    datetime_object = datetime.strptime(s,form)
    time_change = dt.timedelta(hours=12)
    if(ap == 'PM'):
        datetime_object = datetime_object + time_change
    return datetime_object
def drop(df,vcolumns):
    df.drop(columns = vcolumns, inplace=True)
    


def cloc(df,column,op,val):
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
    return df.loc[ ops[op](df[column], val)]



def savecsv_spark(df,path,overwrite=True):
    sparkDF= spark.createDataFrame(df)
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



def ri(df):
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
            df['time'] = df.apply(lambda x: treat_time(x))
        try:
            df.drop(columns = ['Unnamed: 0'],inplace=True)
        except:
            pass
        return df

# retorna um vetor com o nome de todas as colunas menos o tempo com a media, max e min e o novo tempo

def get_amm_column(df,time,nametime="",concantval = ""):
    column_names = list(df.columns)
    final_column_names = []
    initial_column_names = column_names
    csz = len(column_names)
    if(nametime!=""):
          final_column_names.append(nametime)
    for x in range(0,csz):
          if(column_names[x]!="time"):
            final_column_names.append(column_names[x]+"_25"+concantval)
            final_column_names.append(column_names[x]+"_50"+concantval)
            final_column_names.append(column_names[x]+"_75"+concantval)
            final_column_names.append(column_names[x]+"_val"+concantval)
            
        #   final_column_names.append(column_names[x]+"_min"+concantval)
    return final_column_names


# agrupa o dataframe df pelo tempo time
def group(df,time,nametime ="",concatval = ""):
    column_names = list(df.columns)
    final_column_names = get_amm_column(df,time,nametime,concatval)
    print(final_column_names)
    initial_column_names = column_names
    column_names.sort()
    newdf = pd.DataFrame(columns = final_column_names)
    incount = 0
    for i in range(0,len(df)-time,time):
        dfaux = df[i:min(i+time,len(df)-1)]
        ri(dfaux)
        print("here")
        sz_newdf = len(final_column_names)
        newdf.loc[len(newdf.index)]= [0]*len(newdf.columns)
        for x in list(initial_column_names):
                print(x) 
                if(x!=nametime):
                        newdf.loc[incount,x+"_25"+concatval] = float(dfaux[x].quantile([.25]))
                        newdf.loc[incount,x+"_50"+concatval] = float(dfaux[x].quantile([.50]))
                        newdf.loc[incount,x+"_75"+concatval] = float(dfaux[x].quantile([.75]))       
                        print("here")
                        newdf.loc[incount,x+"_next_val"+concatval] = df.loc[min(i+time+1,len(df)-1),x]
                        print("here")               
        if(nametime!=""):
            newdf.loc[incount,nametime]= df.loc[i,nametime]
        clear_output(wait=True)
        print("append",i)
        incount = incount +1
    return newdf

def rsquared(x, y):
    """ Return R^2 where x and y are array-like."""
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    return r_value**2


def group_for_columns(df,dftime,lennew,nlabels,nametime=""):
    newco = []
    get_name = {}
    columns = get_amm_column(df,dftime,nametime)
    for x in columns:
        if(x!=nametime):
            for i in range(dftime,nlabels*dftime+1,dftime):
                newco.append(str(x+str(i)))
                get_name[str(x+str(i))]=i
    #newdf = pd.DataFrame(columns=newco)
    v= []
    for i in range(dftime,nlabels*dftime+1,dftime):
            dfaux = group(df,i,nametime,str(i))
            v.append(dfaux)
            print("feitooo")
    return pd.concat(v)
  

def calculate_r2(df,to_compare,columns = []):
    if(columns == []):
        for x in df.columns:
            if(x!=to_compare):
                    dfaux = df.copy()
                    dfaux.dropna(subset = [x,to_compare],inplace=True)
                    dfaux  = cloc(dfaux,x,'!=',0)
                    dfaux  = cloc(dfaux,to_compare,'!=',0)
                    if(len(dfaux)>0):
                        print('Y = ',to_compare,'X = ',x,'r2',rsquared(dfaux[x],dfaux[to_compare]))
                       # print(dfaux.isnull().sum()) 
                       # print("========================")
                    else:
                        print('EMPTY DF')
def tr_time(form,s = '', df = None, nametime = ''):
    if(s != ''):
        print(s)
        s = s.split(' ')
        ap =  s[-1]
        s = s[0]+' '+s[1]
        datetime_object = datetime.strptime(s,form)
        time_change = dt.timedelta(hours=12)
        if(ap == 'PM'):
            datetime_object = datetime_object + time_change
        return datetime_object
    else:
        df[nametime] = df[nametime].apply(lambda x: tr_time(form,x))


def create_delta(df,nametime):
    fix = df[nametime][0]
    df['delta'] =  df[nametime] - fix
    df['delta'] = df['delta'].apply(lambda x: x.total_seconds())

def treat_rawdata(df, operations,nametime, subset , path = '', treat_time = False, form = "%d/%m/%y %H:%M:%S"):
    dfaux = df.copy()
    for x in operations:
        dfaux = cloc(df,x[0],x[1],x[2])
    print('cloc')
    print(nametime)
    dfaux.dropna(subset = subset, inplace = True)
    if(treat_time):
        tr_time(form, df =  dfaux, nametime = nametime)
    print('tr_time')
    dfaux = dfaux.sort_values(by = nametime)
    print('sort')
    ri(dfaux)
    print('ri')
    create_delta(dfaux, nametime)
    print('create_delta')
    return dfaux

from IPython.display import display_html
from itertools import chain,cycle
def display_side_by_side(*args,titles=cycle([''])):
    html_str=''
    for df,title in zip(args, chain(titles,cycle(['</br>'])) ):
        html_str+='<th style="text-align:center"><td style="vertical-align:top">'
        html_str+=f'<h2 style="text-align: center;">{title}</h2>'
        html_str+=df.to_html().replace('table','table style="display:inline"')
        html_str+='</td></th>'
    display_html(html_str,raw=True)
