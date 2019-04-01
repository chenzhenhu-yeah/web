
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from hu_talib import MA
from stock_util import get_inx_mongo, get_stk_hfq_mongo

def calc_inx(code,base_date, end_date):
    df = get_inx_mongo('399001',base_date, end_date)
    l = len(df)

    #v = df.iat[-1,6]  # amount
    #df['rate'] = round((df['amount']/v -1),4)
    v = df.at[l-1,'volume']
    df['rate_v'] = round((df['volume']/v -1),4)

    c = df.at[l-1,'close']
    df['rate_c'] = round((df['close']/c -1),4)

    df = df.set_index('date')
    #print(df)
    return df


def calc_stk(code,base_date, end_date,df_inx):
    df = get_stk_hfq_mongo(code,base_date, end_date)
    l = len(df)

    v = df.at[l-1,'volume']
    c = df.at[l-1,'close']

    df = df.set_index('date')
    df = df.sort_index()

    df['rate_v'] = round((df['volume']/v -1),4)
    df['rate_v'] = df['rate_v'] - df_inx['rate_v']
    df_v =  MA(df, 5, 'rate_v')
    df_v =  MA(df_v, 20, 'rate_v')
    #print(df_v)

    df['rate_c'] = round((df['close']/c -1),4)
    df['rate_c'] = df['rate_c'] - df_inx['rate_c']
    df_c =  MA(df, 5, 'rate_c')
    df_c =  MA(df_c, 20, 'rate_c')
    #print(df_c)

    return df_v, df_c

def to_bottle(df_v, df_c):
    r = []
    dates = list(df_v.index)
    pre_date = dates.pop(0)
    for date in dates:
        if df_v.at[date,'ma_5'] > df_v.at[date,'ma_20'] and \
           df_v.at[pre_date,'ma_5'] <= df_v.at[pre_date,'ma_20'] and \
           df_c.at[date,'ma_5'] > df_c.at[date,'ma_20'] :
            r.append([date,code])
        pre_date = date

    df = pd.DataFrame(r, columns=['date','code'])
    return df


if __name__ == "__main__":
    base_date = '2017-12-02'
    end_date  = '2018-12-12'

    df_inx = calc_inx('399001',base_date, end_date)

    df_bottle = pd.read_csv('bottle.csv',dtype='str')
    codes = ['002570','300408']
    for code in codes:
        df_v, df_c = calc_stk(code,base_date, end_date, df_inx)
        df_bottle = df_bottle.append(to_bottle(df_v, df_c))

    df_bottle = df_bottle.sort_values('date')
    #print(df_bottle)
    df_bottle.to_csv('bottle.csv', index=False)
