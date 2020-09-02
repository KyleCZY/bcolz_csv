# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 20:10:24 2020

"""

import numpy as np
import pandas as pd
import bcolz
import os
import tushare as ts
import shutil

# ========================== Tushare 认证 =====================================
pro=ts.pro_api('********')

# =============================================================================
#获得交易日序列
def trade_days(start, end):
    start_date = start.replace('-','')
    end_date = end.replace('-','')
    trading_days = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    TD = trading_days[trading_days['is_open'] == 1]
    return TD['cal_date'].values


#比较指定的输入[date,sid]和某行，如果有两个输入（日期+股票），那么视位置返回1，0，-1
def mat_compare(A, B, side='left'):
    n = len(A.dtype)
    assert side in ['left', 'right']
    assert n <= len(B.dtype) and A.dtype.names == B.dtype.names[:n] and A.shape == B.shape
    cmp_value = np.zeros(A.shape, dtype='int64')
    for name in A.dtype.names:
        cmp_value = 2 * cmp_value - 1 * (A[name] < B[name]) + 1 * (A[name] > B[name])
    #如果只有一个输入(日期date),那就需要考虑是包含还是不包含，如果不包含(left)的话，
    #该日期之前为1，该日为0，日期之后为-1，如果包含(right)的话，该日期及之前都为1，之后为-1
    if n < len(B.dtype):
        if side == 'left':
            cmp_value = 2 * cmp_value - 1
        elif side == 'right':
            cmp_value = 2 * cmp_value + 1
    return -1 * (cmp_value < 0) + (cmp_value > 0)


#将指定日期的dataframe格式的数据的列名进行修改        
def test_df(days):
    df_origin = pd.read_csv('Y:\\Astock\\Tushare_data\\'+days+'.csv') # csv数据路径
    df = df_origin[['trade_date', 'ts_code', 'pre_close',
               'open', 'high', 'low',
               'close', 'vol', 'amount',
               'adj_factor','is_open']].copy()
    col_map = {'trade_date': 'date', 'ts_code': 'sid', 'pre_close': 'pre_close',
               'open': 'open', 'high': 'high', 'low': 'low',
               'close': 'close', 'vol': 'vol', 'amount': 'amount',
               'adj_factor':'adj_factor','is_open': 'trade_status',}
    df.rename(columns = col_map, inplace=True)
    return df


#初始化操作
def reset(path, keys=['date', 'sid']):
    col_dtype_dflt = [('date', 'S8', b'00000000'), ('sid', 'S9', b'000000000'),  ('pre_close', 'f8', np.nan),
                      ('open', 'f8', np.nan), ('high', 'f8', np.nan), ('low', 'f8', np.nan),
                      ('close', 'f8', np.nan), ('vol', 'f8', np.nan), ('amount', 'f8', np.nan), 
                      ('adj_factor', 'f8', np.nan), ('trade_status', 'i4', -1)]
    
    if os.path.exists(path):
        shutil.rmtree(path)
    #创建新的ctable文件
    bc = bcolz.fill((0,), dtype=[('_id', 'u8')], rootdir=path, dflt=0, 
                    chunklen = 1024 * 1024 * 16)
    #创建指定的列
    for col, dtype, dflt in col_dtype_dflt:
        bc.addcol([], name = col, dtype = dtype, 
                   dflt = dflt, chunklen = 1024 * 1024 * 16)
            
    bc.attrs['keys'] = keys
    bc.flush()
    print('reseted')
    

#将df转换成array
def trans_df2arr(df):  
    global bc
    assert len(df) > 0
    col_dtype = [(col, bc[col].dtype) for col in df.columns]
    arr = np.zeros(len(df), dtype=col_dtype)
    for col, dtype in col_dtype:
        arr[col] = df[col].values.astype(dtype)
    return arr, list(df.columns)


#写入每日数据
def append_data(df):
    global bc
    arr, cols = trans_df2arr(df)
    #修改table大小和索引
    s, e = bc.len, bc.len + len(df)
    bc.resize(e)  # self.table.flush()
    bc['_id'][s:e] = np.arange(s, e)
    #写入数据
    for col in cols:
        bc[col][s:e] = arr[col]
    bc.flush()


#根据给定输入确定所在索引
def searchsorted(bc, v, side='left'):
    assert isinstance(v, np.void) and side in ['left', 'right']
    A = bc[bc.attrs['keys']]
    l, r = 0, bc.len
    cmp_value = 1 if side == 'left' else 0
    # func = operator.lt if side == 'left' else operator.le
    if r == 0 or mat_compare(v, A[r-1], side) >= cmp_value:
        return r
    while l < r and mat_compare(v, A[l], side) >= cmp_value:
        m = (l+r) // 2
        if mat_compare(v, A[m], side) >= cmp_value:
            l = m + 1
        else:
            r = m
    return l


#获得指定间隔内的所有数据，start和end可以是[日期+股票]
def get_data(bc, start, end = None, cols = None):
    if end is None:
        end = start
    #处理一下数据类型
    st = np.zeros([1,], dtype=[(ele, bc[ele].dtype) for ele in bc.attrs['keys'][:len(start)]])
    st[0] = tuple(start)
    en = np.zeros([1,], dtype=[(ele, bc[ele].dtype) for ele in bc.attrs['keys'][:len(end)]])
    en[0] = tuple(end)
    #确定所在索引
    s = searchsorted(bc, st[0])
    e = searchsorted(bc, en[0], side='right')
    if cols is None:
        cols = [ele for ele in bc.names if ele != '_id']    
    return pd.DataFrame(bc[cols][s:e])


# =============================================================================   

if __name__ == '__main__':    
    
    # ======================= Create data =====================================
    path = 'V:\\stockdata'  #存放bcolz数据的路径
    reset(path)
    
    trade_days = trade_days('20100104','20130104')
    bc = bcolz.open(path, mode='a')
    for days in trade_days:
        append_data(test_df(days))
        print('{} finished'.format(days))
    
    # ======================= Read data =======================================
    bc = bcolz.open(path, mode='r')
    start = ['20100105','601766.SH']
    end = ['20100111','601900.SH']
    data = get_data(bc, start, end)