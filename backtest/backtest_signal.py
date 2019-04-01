
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import ipdb
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_001']
df_signal = pd.DataFrame(columns=['code','date','close','MACD','DIFF','DEA'])


def MACD(df, n_fast=12, n_slow=26, ksgn='close'):
    '''
    def MACD(df, n_fast, n_slow):
      #MACD指标信号和MACD的区别, MACD Signal and MACD difference
	MACD是查拉尔·阿佩尔(Geral Appel)于1979年提出的，由一快及一慢指数移动平均（EMA）之间的差计算出来。
	“快”指短时期的EMA，而“慢”则指长时期的EMA，最常用的是12及26日EMA：

    【输入】
        df, pd.dataframe格式数据源
        n，时间长度
        ksgn，列名，一般是：close收盘价
    【输出】
        df, pd.dataframe格式数据源,
        增加了3栏：macd,sign,mdiff
    '''
    EMAfast = pd.Series(df[ksgn].ewm(span = n_fast).mean())
    EMAslow = pd.Series(df[ksgn].ewm(span = n_slow).mean())

    MACDdiff = pd.Series(EMAfast - EMAslow, name='DIFF')
    MACDdea = pd.Series(MACDdiff.ewm(span = 9).mean(), name='DEA')
    MACD = pd.Series((MACDdiff - MACDdea)*2, name = 'MACD')

    df = df.join(MACD)
    df = df.join(MACDdiff)
    df = df.join(MACDdea)
    return df

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

def get_signal(code,_date):
    #return df: columns=[date,macd]
    df1 = df_signal[(df_signal.code==code) & (df_signal.date<=_date)]
    df1.sort_values(by='date')
    #print(df1.tail())
    df1 = df1.loc[:,['date','MACD']]
    #ipdb.set_trace()
    return df1

def sell_signal(stock_pool, _date):
    r = []
    codes = list(stock_pool.index)
    for code in codes:
        df1 = get_signal(code,_date) #columns=[date,macd]
        if len(df1) >= 30:
            if df1.iat[-1,0] == _date:
                if df1.iat[-1,1] <= 0 and df1.iat[-2,1] > 0:
                    r.append(code)
    return r

def buy_signal(stock_pool, _date):
    r = []
    codes = list(stock_pool.index)
    for code in codes:
        df1 = get_signal(code,_date) #columns=[date,macd]
        if len(df1) >= 30:
            if df1.iat[-1,0] == _date:
                if df1.iat[-1,1] > 0 and df1.iat[-2,1] <= 0:
                    df2 = df1.iloc[-21:-1,:]
                    if len(df2[df2.MACD>0]) in [3,4,5,6,7,8,9]:
                        r.append(code)
                        #print(df1)
    return r

def init_macd(stock_pool):
    global df_signal

    codes = list(stock_pool.index)
    for code in codes:
        #print(df_signal)
        df1 = df_signal[df_signal.code==code]
        if len(df1) == 0:
            trade_dates = get_trading_dates()
            stocks = DB_CONN['daily'].find(
                    {'code':code , 'date':{'$in': trade_dates}},
                    sort=[('date', ASCENDING)],
                    projection={'date': True, 'close': True, '_id':False})
            #print(list(stocks))
            df1 = pd.DataFrame(list(stocks), columns=['date','close'])
            if len(df1) >= 30:
                df1 = MACD(df1)
                #print(df1)
                df1['code'] = code
                df_signal = df_signal.append(df1,sort=False)
                #print(df_signal)
                #ipdb.set_trace()

def backtest(begin_date, end_date, stock_pool):
    """
    :param begin_date: 回测开始日期
    :param end_date: 回测结束日期
    """

    all_dates = get_trading_dates(begin_date, end_date)

    # 按照日期一步步回测
    for i, _date in enumerate(all_dates):
        print('Backtest at %s.' % _date)

        if i == 0:
            init_macd(stock_pool)

        if i != 0 :
            #卖出信号及操作
            to_sell_codes = sell_signal(stock_pool, _date)
            for code in to_sell_codes:
                print('                       ****** sell signal '+ code)

            #买入出信号及操作
            to_buy_codes = buy_signal(stock_pool, _date)
            for code in to_buy_codes:
                print('                       ****** buy signal '+ code)

def run():
    filename = 'care_stocks.csv'

    now = datetime.now()
    end_date = now.strftime('%Y-%m-%d')
    begin_date = (now-timedelta(days=15)).strftime('%Y-%m-%d')
    #end_date = '2018-10-21'
    #begin_date = '2018-10-11'

    stock_pool = pd.read_csv(filename,dtype={'code':str})
    stock_pool = stock_pool[['code']]
    stock_pool = stock_pool.drop_duplicates()
    stock_pool = stock_pool.set_index('code')
    #print(stock_pool)
    backtest(begin_date, end_date, stock_pool)

if __name__ == "__main__":
    run()
