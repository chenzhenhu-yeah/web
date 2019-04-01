import os
import re
import datetime
import time
import io
import pymongo
import zlib
import pandas as pd

conn = pymongo.MongoClient('localhost', 27017)
db = conn.stocks         #连接mydb数据库，没有则自动创建
cols = db.k


def get_detail(rec):
    b = zlib.decompress(rec['detail'])
    s = b.decode()
    return s

def get_mongo_csv_df(rec):
    fss = io.StringIO(get_detail(rec))
    xd0 = pd.read_csv(fss,dtype={'code':'str'})
    #xd0 = pd.read_csv(fss,encoding='gbk')
    return xd0

def extract_single(code,date,dt_1,dt_2):
    r = False
    rec = cols.find_one({'code':code})
    if rec:
        df = get_mongo_csv_df(rec)
        df = df[df.date<=date]
        df = df.sort_values(by=['date'])
        df['10d'] = df['close'].rolling(10).mean()
        df['30d'] = df['close'].rolling(30).mean()
        df = df.loc[:,['date','close','10d','30d']]
        df = df.sort_values(by=['date'], ascending=False)
        if df.iat[0,1]>=df.iat[0,2] and df.iat[0,2]>=df.iat[0,3] and df.iat[0,0] in [date,dt_1,dt_2]:
            r = True
    else:
        print('empty')
    #df.to_csv('a1.csv',index=False)
    return r

date = '2018-08-17'
dt_1 = '2018-08-16'
dt_2 = '2018-08-15'

df = pd.read_csv('stk_cyb.csv',dtype='str',encoding='gbk')
codes = df['code']
for code in codes:
    if extract_single(code,date,dt_1,dt_2):
        print(code)
