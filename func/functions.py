from datetime import datetime, timedelta
import pandas as pd
import pyspark.pandas as ps
import datetime as dt
from datetime import datetime
import numpy as np
import operator
from IPython.display import clear_output
from pyspark.sql import SparkSession
import operator
import matplotlib.pyplot as plt
from scipy import stats
import os
# assign directory



def ctag(df,name_target):
    df[name_target] = np.nan

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

def tr_time(form = "%d/%m/%y %H:%M:%S",s = '', df = None, nametime = ''):
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



def dropc(df,vcolumns):
    df.drop(columns = vcolumns, inplace=True)
    


def cloc(df,eq,val):
    eq = eq.split(" ")
    eq
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
    return df.loc[ ops[eq[1]](df[eq[0]],val)]



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
            tr_time(form, df =  dfaux, nametime = nametime)
        if(nametime != ''):
            df[nametime] = pd.to_datetime(df[nametime])
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
        dfaux = cloc(dfaux, nametime, '<=', dfaux[nametime][0] + timedelta(seconds=time))
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
  

def calculate_r2(df,to_compare,columns = [], nametime = ''):
    if(columns == []):
        for x in df.columns:
            if(x!=to_compare and x!=nametime):
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
def tr_time(form = "%d/%m/%y %H:%M:%S",s = '', df = None, nametime = ''):
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

        
def get_splited(df,sep,target, shuff = False):
    dfaux = df.copy()
    if(shuff):
        dfaux  = dfaux.sample(frac=1).reset_index(drop=True)
    val_sep = int(dfaux.shape[0]*sep)
    y =  dfaux[target].copy()
    dfaux.drop(columns =[target],inplace=True) 
    x_train = dfaux.iloc[:val_sep]
    x_test = dfaux.iloc[val_sep:]
    y_train = y.iloc[:val_sep]
    y_test = y.iloc[val_sep:]
        
    return x_train,y_train,x_test,y_test  




import matplotlib.pyplot as plt
import numpy as np
def bplot(df,x,y,be=-1,end=-1):
    if(be!=-1 and end!=-1):
        xpoints = df[x][be:end]
        ypoints = df[y][be:end]
        
    else:
        xpoints = df[x]
        ypoints = df[y]
    plt.scatter(xpoints, ypoints)
    plt.show()
