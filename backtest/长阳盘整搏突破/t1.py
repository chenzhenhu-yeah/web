
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne

import sys
sys.path.append(r'../../')
from quant_001.hu_talib import MA, MACD
from quant_001.stock_util import get_inx_mongo, get_stk_hfq_mongo, get_trading_dates_mongo
from quant_001.database import DB_CONN


def validate_stk(code):
    r = []
    begin_date = '2019-02-22'
    df = get_stk_hfq_mongo(code,begin_date)
    df1 = df[df.date <= '2019-03-20']
    price_g = df1['high'].max()
    #print(code,price_g)
    df2 = df[df.date > '2019-03-20']
    df2 = df2.sort_values('date')

    for i,row in df2.iterrows():
        #print(row.date)
        if row['close'] >= price_g:
            df3 = df2[df2.date > row.date]
            if df3['close'].min() >= price_g:
                r.append([code, row.date])
                #print(df3)
                break
    return r

if __name__ == '__main__':
    r = []

    df = pd.read_csv('stk_cyb.csv', dtype='str', encoding='gbk')
    codes = list(df['code'])
    #codes = ['002570','300461']
    print(codes)
    for code in codes:
        r += validate_stk(code)

    df = pd.DataFrame(r,columns=['code','date'])
    df.to_csv('b1.csv',index=False)
    #print(r)
