
import time
import os
import pandas as pd
import tushare as ts

def show_trade():
    df = pd.read_csv('log/trade_record/pingan.csv', sep='&')
    orders = list(df.ins)
    d = [eval(x) for x in orders ]

    #df = pd.DataFrame(orders, columns=['ins','portfolio','code','num','price','cost','agent','name'])
    df = pd.DataFrame(d)
    df = df.loc[df.ins.isin(['sell_order','buy_order']),['ins', 'code', 'num', 'cost']]
    #print(df)
    codes = set(df.code)
    r = []
    for code in codes:
        df1 = ts.get_realtime_quotes(code)
        name = df1.at[0,'name']

        df1_buy = df[(df.code==code)&(df.ins=='buy_order')]
        df1_sell = df[(df.code==code)&(df.ins=='sell_order')]
        buynum = df1_buy.num.sum()
        buycost = df1_buy.cost.sum()
        sellnum = df1_sell.num.sum()
        sellcost = df1_sell.cost.sum()

        if buynum == sellnum:
            #r.append([code,buynum,buycost,sellnum,sellcost, sellcost-buycost])
            r.append([code, name, sellcost-buycost])

    return r

r = show_trade()
#print(r)
df = pd.DataFrame(r, columns=['code','name', 'profit'])
df.to_excel('e1.xls')
