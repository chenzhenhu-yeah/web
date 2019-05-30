import os
import re
import datetime
import time
import io
import pymongo
import zlib
import numpy as np
import pandas as pd
import tushare as ts
import ipdb

conn = pymongo.MongoClient('localhost', 27017)
db = conn.companys         #连接mydb数据库，没有则自动创建
cols = db.profit

years =  [2019,2018,2017,2016,2015]

def get_detail(rec):
    b = zlib.decompress(rec['detail'])
    s = b.decode()
    return s

def get_mongo_csv_df(rec):
    fss = io.StringIO(get_detail(rec))
    xd0 = pd.read_csv(fss,dtype={'code':'str'})
    #xd0 = pd.read_csv(fss,encoding='gbk')
    return xd0

def extract_single(y,q):
    tick = cols.find_one({'year':y,'quarter':q})
    if tick:
        df = get_mongo_csv_df(tick)
        return df
    else:
        #print('empty')
        return None

def get_tb(df):
    s0 = round((df.loc[years[0],:]/df.loc[years[1],:]-1)*100,2)
    s1 = round((df.loc[years[1],:]/df.loc[years[2],:]-1)*100,2)
    s2 = round((df.loc[years[2],:]/df.loc[years[3],:]-1)*100,2)
    df1 = pd.DataFrame([s0,s1,s2],index=[years[0],years[1],years[2]],columns=['Q1','Q2','Q3','Q4'])
    return df1


def built_profit(code):
    '''
    构建净利润矩阵
    '''
    result = []
    for y in years:
        row = []
        for q in [1,2,3,4]:
            df1 = extract_single(y,q)
            if df1 is not None:
                df1 = df1[df1.code == code]
                #print(df1)
                if df1.empty:
                    row.append(np.nan)
                else:
                    row.append(df1.iat[0,-4])       #净利润(net profit)
            else:
                row.append(np.nan)
        r = []
        r.append(round(row[0]/100,2))               #一季度单季，单位：亿
        r.append(round((row[1] - row[0])/100,2))    #二季度单季，单位：亿
        r.append(round((row[2] - row[1])/100,2))    #三季度单季，单位：亿
        r.append(round((row[3] - row[2])/100,2))    #四季度单季，单位：亿
        result.append(r)

    df = pd.DataFrame(result,index=years,columns=['Q1','Q2','Q3','Q4'])
    return df

def built_bi(code):
    '''
    构建营收矩阵
    '''
    result = []
    for y in years:
        row = []
        for q in [1,2,3,4]:
            df1 = extract_single(y,q)
            if df1 is not None:
                df1 = df1[df1.code == code]
                #print(df1)
                if df1.empty:
                    row.append(np.nan)
                else:
                    row.append(df1.iat[0,-2])       #营业收入(business income)
            else:
                row.append(np.nan)
        r = []
        r.append(round(row[0]/100,2))               #一季度单季，单位：亿
        r.append(round((row[1] - row[0])/100,2))    #二季度单季，单位：亿
        r.append(round((row[2] - row[1])/100,2))    #三季度单季，单位：亿
        r.append(round((row[3] - row[2])/100,2))    #四季度单季，单位：亿
        result.append(r)

    df = pd.DataFrame(result,index=years,columns=['Q1','Q2','Q3','Q4'])
    return df

def choose_bi(codes):
    pre_y, pre_q = 0,0
    cur_y, cur_q = 0,1
    r = []

    for code in codes:
        df = built_bi(code)
        df1 = get_tb(df)
        if df1.iat[pre_y, pre_q]>30 and df1.iat[cur_y, cur_q]>50:
            r.append([code, df1.iat[pre_y, pre_q], df1.iat[cur_y, cur_q]])

    df = pd.DataFrame(r,columns=['code','pre','cur'])
    return df


def choose_profit(codes):
    pre_y, pre_q = 0,0
    cur_y, cur_q = 0,1
    r = []

    for code in codes:
        df = built_profit(code)
        df1 = get_tb(df)
        if df1.iat[pre_y, pre_q]>30 and df1.iat[cur_y, cur_q]>50:
            r.append([code, df1.iat[pre_y, pre_q], df1.iat[cur_y, cur_q]])

    df = pd.DataFrame(r,columns=['code','pre','cur'])
    return df


def built_gross(code):
    '''
    构建毛利率矩阵
    '''
    result = []
    for y in years:
        row = []
        for q in [1,2,3,4]:
            df1 = extract_single(y,q)
            if df1 is not None:
                df1 = df1[df1.code == code]
                #print(df1)
                if df1.empty:
                    row.append(np.nan)
                else:
                    row.append(df1.iat[0,4])
            else:
                row.append(np.nan)
        result.append(row)

    df = pd.DataFrame(result,index=years,columns=['Q1','Q2','Q3','Q4'])
    return df

def choose_compare_Q(codes, pre_y, pre_q, cur_y, cur_q):
    r = []
    i=0
    stage = 0.1
    total = len(codes)

    for code in codes:
        i += 1
        if i/total > stage:
            print(stage)
            stage += 0.1

        df_gross = built_gross(code)
        # 毛利率大于30%
        if df_gross.iat[pre_y, pre_q]>30:
            df_bi = built_bi(code)
            df1 = get_tb(df_bi)
            # 营收增速分别大于30%、50%
            if df1.iat[pre_y, pre_q]>30 and df1.iat[cur_y, cur_q]>50:
                df_profit = built_profit(code)
                df2 = get_tb(df_profit)
                # 净利润增速分别大于30%、50%
                if df2.iat[pre_y, pre_q]>30 and df2.iat[cur_y, cur_q]>30:
                    r.append([code, df1.iat[pre_y, pre_q], df1.iat[cur_y, cur_q],df2.iat[pre_y, pre_q], df2.iat[cur_y, cur_q]])

    df = pd.DataFrame(r,columns=['code','pre_bi','cur_bi','pre_profit','cur_profit'])
    return df


def choose_single_Q(codes, cur_y, cur_q):
    print('begin...')
    r = []
    i=0
    stage = 0.1
    total = len(codes)

    for code in codes:
        i += 1
        if i/total > stage:
            print(stage)
            stage += 0.1

        df_gross = built_gross(code)
        #ipdb.set_trace()
        if df_gross.iat[cur_y, cur_q]>30:
            df_bi = built_bi(code)
            df1 = get_tb(df_bi)
            #ipdb.set_trace()
            if df1.iat[cur_y, cur_q]>30:
                df_profit = built_profit(code)
                df2 = get_tb(df_profit)
                if df2.iat[cur_y, cur_q]>50:
                    r.append([code, df1.iat[cur_y, cur_q],df2.iat[cur_y, cur_q]])

    df = pd.DataFrame(r,columns=['code','cur_bi','cur_profit'])
    return df

if __name__ == "__main__":
    #stock_df = pd.read_csv('stk_cyb.csv',dtype='str',encoding='gbk')
    #df = pd.read_csv('stk_zxb.csv',dtype='str',encoding='gbk')
    #df = pd.read_csv('stk_sz.csv',dtype='str',encoding='gbk')

    #stock_df = ts.get_stock_basics()
    #stock_df.to_csv('all_stock.csv')
    stock_df = pd.read_csv('all_stock.csv', dtype={'code':str})
    stock_df = stock_df.set_index('code')
    codes = list(stock_df.index)

    pre_y, pre_q = 1,3    #Q2
    cur_y, cur_q = 0,0    #Q3
    df1 = choose_compare_Q(codes, pre_y, pre_q, cur_y, cur_q)
    #cur_y, cur_q = 0,2      #Q3
    #df1 = choose_single_Q(codes, cur_y, cur_q)
    df1.to_csv('a2.csv',index=False)
    df1 = df1.set_index('code')

    df2 = stock_df
    df2 = df2[df2.index.isin(df1.index)]
    df2 = df2.loc[:,['name','industry','area','pe']]
    df2 = df2.join(df1)
    #df2.to_csv('sx_income_single_2018Q3.csv')
    df2.to_csv('sx_income_compare_2019Q1.csv')
