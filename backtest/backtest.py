
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
#import ipdb
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne

import sys
sys.path.append(r'../')
from quant_001.hu_talib import MA, MACD
from quant_001.stock_util import get_inx_mongo, get_stk_hfq_mongo, get_trading_dates_mongo


DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_001']

cash = 1E8
single_position = 200E4


def get_signal(df_signal,code,_date):
    #return df: columns=[date,macd]
    df1 = df_signal[(df_signal.code==code) & (df_signal.date<=_date)]
    df1 = df1.sort_values(by='date')
    #print(df1.tail())
    df1 = df1.loc[:,['date','MACD']]
    #ipdb.set_trace()
    return df1

def sell_signal(df_signal,codes, _date):
    r = []
    for code in codes:
        df1 = get_signal(df_signal,code,_date) #columns=[date,macd]
        if len(df1) >= 30:
            if df1.iat[-1,0] == _date:
                if df1.iat[-1,1] <= 0 and df1.iat[-2,1] > 0:
                    r.append(code)
    return r

def buy_signal(df_signal,codes, _date):
    r = []
    for code in codes:
        df1 = get_signal(df_signal,code,_date) #columns=[date,macd]
        if len(df1) >= 30:
            if df1.iat[-1,0] == _date:
                if df1.iat[-1,1] >= 0 and df1.iat[-2,1] < 0:
                    if df1.iat[-3,1] < 0 and df1.iat[-4,1] < 0 and df1.iat[-5,1] < 0 and df1.iat[-6,1] < 0 :
                        df2 = df1.iloc[-21:-1,:]
                        if len(df2[df2.MACD>0]) in [3,4,5,6,7,8,9]:
                            r.append(code)
                            #print(df1)
    return r

def calc_signal(df_signal, codes, flag):
    df_s = df_signal
    for code in codes:
        df_code = df_signal[df_signal.code == code]
        if len(df_code) == 0:
            df1 = get_stk_hfq_mongo(code)
            df1 = df1[['date','close']]
            df1 = df1.sort_values(by='date')
            if len(df1) >= 30:
                if flag == 'MACD':
                    df1 = MACD(df1)
                #print(code)
                #print(df1)
                df1['code'] = code
                df_s = df_s.append(df1,sort=False)
    return df_s

def sell_stk(stk_holds,code,_date):
    df_r = stk_holds
    df_hold = stk_holds[stk_holds.index==code]
    if len(df_hold) > 0: #有持仓，则卖出
        df1 = get_stk_hfq_mongo(code,_date,_date)
        price = df1.at[0,'close']
        num = df_hold.at[code,'num']
        value = num*price
        print('******** sell stock '+ str((code, value-df_hold.at[code,'cost'])))

        global cash
        cash += value
        df_r = stk_holds.drop(index=code)
    return df_r

def buy_stk(stk_holds,code,_date):
    df_r = stk_holds
    df_hold = stk_holds[stk_holds.index==code]
    if len(df_hold) == 0: #无持仓，则买入
        df1 = get_stk_hfq_mongo(code,_date,_date)
        price = df1.at[0,'close']
        num = int(single_position/price/100)*100
        if num >= 100:
            cost = num*price

            global cash
            if cash-cost >= 0:
                cash -= cost
                df_r = stk_holds.append(pd.DataFrame([[num,cost]],columns=['num','cost'],index=[code]),sort=False)
                print('******* buy stock '+ str((code, num, cost)))
            else:
                print('buy stock '+code+', but cash not enough')

    return df_r

def backtest(begin_date, end_date):
    """
    :param begin_date: 回测开始日期
    :param end_date: 回测结束日期
    """

    all_dates = get_trading_dates_mongo(begin_date, end_date)
    df = pd.read_csv('bottle.csv',dtype='str')
    codes = set()
    df_signal = pd.DataFrame([],columns=['code','date','close','MACD','DIFF','DEA'])
    stk_holds = pd.DataFrame([],columns=['num','cost'])  # index='code'


    # 按照日期一步步回测
    for i, _date in enumerate(all_dates):
        #print('Backtest at %s.' % _date, codes)

        # 入池
        df1 =  df[df.date==_date]
        codes.update(set(df1['code']))

        # 准备池中个股的MACD
        df_signal = calc_signal(df_signal,codes,'MACD')

        #卖出信号及操作
        to_sell_codes = sell_signal(df_signal,codes, _date)
        for code in to_sell_codes:
            print('                       ****** sell signal '+ code +_date)
            stk_holds = sell_stk(stk_holds,code,_date)
            print(stk_holds)


        #买入出信号及操作
        to_buy_codes = buy_signal(df_signal,codes, _date)
        for code in to_buy_codes:
            print('                       ****** buy signal '+ code +_date)
            stk_holds = buy_stk(stk_holds,code,_date)
            print(stk_holds)

    print('cash: ',cash)
    print(stk_holds)

def run():

    now = datetime.now()
    end_date = now.strftime('%Y-%m-%d')
    begin_date = (now-timedelta(days=15)).strftime('%Y-%m-%d')

    begin_date = '2018-01-01'
    end_date = '2018-12-21'


    backtest(begin_date, end_date)

if __name__ == "__main__":
    run()
