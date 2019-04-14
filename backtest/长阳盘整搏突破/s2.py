
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne

import sys
sys.path.append(r'../../')
from quant_001.hu_talib import MA, MACD
from quant_001.stock_util import get_inx_mongo, get_stk_hfq_mongo, get_trading_dates_mongo
from quant_001.database import DB_CONN


def keep(code, date):
    r = True

    df = get_stk_hfq_mongo(code,date)
    n = len(df)
    #print(code,date,n)
    if n > 0:
        price_g = df.loc[n-1,'open']
        df1 =df.loc[:n-1,:]
        min_low = df1['low'].min()
        if min_low < price_g:
            r = False

    return r

if __name__ == '__main__':
    r = []

    df = pd.read_csv('b1.csv',dtype='str')
    for i,row in df.iterrows():
        if keep(row.code, row.date):
            r.append([row.code, row.date])

    df = pd.DataFrame(r,columns=['code','date'])
    df.to_csv('b2.csv',index=False)
