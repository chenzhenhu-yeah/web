#  -*- coding: utf-8 -*-

import io
import zlib
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta

import ipdb
import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_001']

def get_trading_dates(begin_date=None, end_date=None):
    """
    获取指定日期范围的按照正序排列的交易日列表
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: 日期列表
    """
    # 开始日期，默认今天向前的365个自然日
    now = datetime.now()
    if begin_date is None:
        one_year_ago = now - timedelta(days=365)
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    daily_cursor = DB_CONN.daily.find(
        {'code': '000001', 'date': {'$gte': begin_date, '$lte': end_date}, 'index': True},
        sort=[('date', ASCENDING)],
        projection={'date': True, '_id': False})

    dates = [x['date'] for x in daily_cursor]

    return dates

def get_detail(rec):
    b = zlib.decompress(rec['detail'])
    s = b.decode()
    return s

def get_mongo_csv_df(rec):
    fss = io.StringIO(get_detail(rec))
    xd0 = pd.read_csv(fss,dtype={'code':'str'})
    #xd0 = pd.read_csv(fss,encoding='gbk')
    return xd0

def get_2b_df(end_date):
    end_date = datetime.strptime(end_date,'%Y-%m-%d')
    begin_date = end_date - timedelta(days=180)

    end_date = end_date.strftime('%Y-%m-%d')
    begin_date = begin_date.strftime('%Y-%m-%d')

    dates = get_trading_dates(begin_date,end_date)
    #print(dates)

    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.stocks
    cols = db.daily

    df_now = get_mongo_csv_df(cols.find_one({'date':dates[-1]}))
    df_last = get_mongo_csv_df(cols.find_one({'date':dates[-2]}))
    df_pre_last = get_mongo_csv_df(cols.find_one({'date':dates[-3]}))

    col = ['code','volume']
    df_now = df_now.loc[:,col]
    df_last = df_last.loc[:,col]
    df_pre_last = df_pre_last.loc[:,col]

    df = pd.merge(df_pre_last,df_last,on='code')
    df = df[df.volume_x > 0]
    df = df[df.volume_y/df.volume_x > 2]
    df = df[df.volume_y/df.volume_x < 7]

    df = pd.merge(df,df_now,on='code')
    df = df[df.volume/df.volume_y > 2]
    df = df[df.volume/df.volume_y < 7]
    return df


def MA(df, n,ksgn='close'):
    '''
    def MA(df, n,ksgn='close'):
    #Moving Average
    MA是简单平均线，也就是平常说的均线
    【输入】
        df, pd.dataframe格式数据源
        n，时间长度
        ksgn，列名，一般是：close收盘价
    【输出】
        df, pd.dataframe格式数据源,
        增加了一栏：ma_{n}，均线数据
    '''
    xnam='ma_{n}'.format(n=n)
    #ds5 = pd.Series(pd.rolling_mean(df[ksgn], n), name =xnam)
    ds2=pd.Series(df[ksgn], name =xnam);
    ds5 = ds2.rolling(center=False,window=n).mean()
    #print(ds5.head()); print(df.head())
    df = df.join(ds5)

    return df

def get_ma_df(end_date,df):
    end_date = datetime.strptime(end_date,'%Y-%m-%d')
    begin_date = end_date - timedelta(days=360)

    end_date = end_date.strftime('%Y-%m-%d')
    begin_date = begin_date.strftime('%Y-%m-%d')

    dates = get_trading_dates(begin_date,end_date)
    #print(dates)

    r = []
    codes = list(df['code'])
    for code in codes:
        daily_cursor = DB_CONN.daily.find(
            {'code': code, 'date': {'$in':dates}, 'index': False},
            sort=[('date', ASCENDING)],
            projection={'date': True, 'close': True, '_id': False})
        df1 = pd.DataFrame(list(daily_cursor),columns=['date','close'])
        df5 = MA(df1,5)
        df60 = MA(df1,60)
        #print(df5.head())
        if df1.iat[-1,1]>=df5.iat[-1,2] and df5.iat[-1,2]>=df60.iat[-1,2]:
            r.append(code)

    #ipdb.set_trace()
    return df[df.code.isin(r)]

if __name__ == "__main__":
    r = []
    dates = get_trading_dates(begin_date='2018-10-13', end_date=None)
    #dates = get_trading_dates(begin_date='2018-03-01', end_date='2018-03-11')
    for end_date in dates:
        df = get_2b_df(end_date)
        df = get_ma_df(end_date,df)
        #print(end_date,df)
        codes = df['code']
        for code in  codes:
            r.append({'date':end_date, 'code':code})
            print({'date':end_date, 'code':code})
    df = pd.DataFrame(r)
    df.to_csv('a1.csv', index=False)
