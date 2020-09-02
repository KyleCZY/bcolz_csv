# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 18:10:01 2020

@author: win10
"""

import time
import bcolz_czy
import pandas as pd
import bcolz


path = 'V:\\stockdata'

start = ['20100104']
end = ['20130104']

time_start = time.time() #开始计时

df_origin = pd.read_csv('V:\\bcolz\\readcsv.csv')
    
time_end1 = time.time()    #结束计时

bc = bcolz.open(path, mode='r')
data = bcolz_czy.get_data(bc, start, end)

time_end2 = time.time()

time_csv= time_end1 - time_start  
time_bcolz= time_end2 - time_end1  

print('csv:', time_csv)
print('bcolz:',time_bcolz)
print('ratio:',time_csv/time_bcolz)

'''
path = 'V:\\stockdata'
bc = bcolz.open(path, mode='r')
trade_day = bcolz_czy.trade_days('20100104','20130104')
start = ['20100104']
end = ['20130104']

time_start = time.time() #开始计时
df = pd.DataFrame()
for days in trade_day:
    df_origin = pd.read_csv('Y:\\Astock\\Tushare_data\\'+days+'.csv')
    df = df.append(df_origin,ignore_index = True)
    print(days)
df.to_csv('V:\\bcolz\\readcsv.csv')
time_end1 = time.time()    #结束计时

data = bcolz_czy.get_data(bc, start, end)

time_end2 = time.time()

time_csv= time_end1 - time_start  
time_bcolz= time_end2 - time_end1  
print(time_bcolz, time_csv, time_csv/time_bcolz)
'''