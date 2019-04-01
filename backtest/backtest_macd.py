
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import ipdb

from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_001']


cash = 1000E4
single_position = 100E4
df_signal = pd.DataFrame(columns=['code','date','close','MACD','DIFF','DEA'])


def MACD(df, n_fast, n_slow,ksgn='close'):
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

def calc_current_value(stock_pool,_date):
    total_value = 0.0
    holding_codes = list(stock_pool.index)
    #print(holding_codes)

    holding_daily_cursor = DB_CONN['daily'].find(
        {'code': {'$in': holding_codes}, 'date': _date},
        projection={'close': True, 'code': True, '_id':False})

    for holding_daily in holding_daily_cursor:
        code = holding_daily['code']
        holding_stock_volume = stock_pool.loc[code,'vol']
        value = holding_daily['close'] * holding_stock_volume
        stock_pool.loc[code,'value'] = value
        #ipdb.set_trace()

    for code in  holding_codes:
        value = stock_pool.loc[code,'value']
        if value < 0:  # 起始日不能有停牌的股票！！！
            stock_pool.pop(code)
            print(code + 'stop trade today')
        total_value += value
        #print('持仓: %s, %10.2f' % (code, value))

    return total_value

def get_signal(code,_date):
    #return df: columns=[date,macd]
    df1 = df_signal[(df_signal.code==code) & (df_signal.date<=_date)]
    df1.sort_values(by='date')
    #print(df1.tail())
    df1 = df1.loc[:,['date','MACD']]
    #ipdb.set_trace()
    return df1

def ajust_pool(stock_pool, _date):
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
                df1 = MACD(df1,12,26)
                df1['code'] = code
                df_signal = df_signal.append(df1,sort=False)
                #print(df_signal)
                #ipdb.set_trace()

def sell_signal_old(stock_pool, _date):
    r = []
    trade_dates = get_trading_dates(None, _date)
    codes = list(stock_pool.index)
    for code in codes:
        stocks = DB_CONN['daily'].find(
                {'code':code , 'date':{'$in': trade_dates}},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id':False})
        #print(list(stocks))
        df1 = pd.DataFrame(list(stocks), columns=['date','close'])
        if len(df1) >= 30:
            if df1.iat[-1,0] == _date:
                df1 = MACD(df1,12,26)
                #print(df1)
                if df1.iat[-1,2] <= 0 and df1.iat[-2,2] > 0:
                    r.append(code)
    return r

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

def sell_stock(stock_pool,code,_date):
    if stock_pool.loc[code,'vol'] > 0: #有持仓，则卖出
        close_price = DB_CONN['daily'].find_one(
                {'code':code , 'date':_date},
                projection={'close': True})['close']

        vol = stock_pool.loc[code,'vol']
        value = vol*close_price
        print('***************************** sell stock '+ str((code, value-stock_pool.loc[code,'cost'])))

        global cash
        cash += value
        stock_pool.loc[code,'vol'] = 0
        stock_pool.loc[code,'cost'] = 0
        stock_pool.loc[code,'value'] = 0
        #print(stock_pool)

def buy_signal_old(stock_pool, _date):
    r = []
    trade_dates = get_trading_dates(None, _date)
    codes = list(stock_pool.index)
    for code in codes:
        stocks = DB_CONN['daily'].find(
                {'code':code , 'date':{'$in': trade_dates}},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id':False})
        #print(list(stocks))
        df1 = pd.DataFrame(list(stocks), columns=['date','close'])
        if len(df1) >= 30:
            if df1.iat[-1,0] == _date:
                df1 = MACD(df1,12,26)
                if df1.iat[-1,2] > 0 and df1.iat[-2,2] <= 0:
                    df2 = df1.iloc[-21:-1,:]
                    if len(df2[df2.MACD>0]) in [3,4,5]:
                        r.append(code)
                        #print(df1)
                        #ipdb.set_trace()
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

def buy_stock(stock_pool,code,_date):
    if stock_pool.loc[code,'vol'] == 0: #无持仓，则买入
        close_price = DB_CONN['daily'].find_one(
                {'code':code , 'date':_date},
                projection={'close': True})['close']
        vol = int(single_position/close_price/100)*100
        if vol >= 100:
            cost = vol*close_price

            global cash
            if cash-cost >0:
                cash -= cost
                stock_pool.loc[code,'vol'] = vol
                stock_pool.loc[code,'cost'] = cost
                print('***************************** buy stock '+ str((code, vol, cost)))
        else:
            print('buy stock '+code+', but cash not enough')

def backtest(begin_date, end_date,stock_pool,base):
    """
    策略回测。结束后打印出收益曲线(沪深300基准)、年化收益、最大回撤、

    :param begin_date: 回测开始日期
    :param end_date: 回测结束日期
    """

    df_profit = pd.DataFrame(columns=['net_value','portfolio_profit', 'base_profit'])
    all_dates = get_trading_dates(begin_date, end_date)

    portfolio_beging_capital = 0
    base_begin_value = 0

    # 按照日期一步步回测
    for i, _date in enumerate(all_dates):
        print('Backtest at %s.' % _date)

        if i != 0 : #首日为基准日
            #调整股票池
            ajust_pool(stock_pool, _date)

            #卖出信号及操作
            to_sell_codes = sell_signal(stock_pool, _date)
            for code in to_sell_codes:
                print('***************************** sell signal '+ code)
                sell_stock(stock_pool,code,_date)

            #买入出信号及操作
            to_buy_codes = buy_signal(stock_pool, _date)
            for code in to_buy_codes:
                print('***************************** buy signal '+ code)
                buy_stock(stock_pool,code,_date)
                #ipdb.set_trace()

        # 计算总资产
        portfolio_current_capital = cash + calc_current_value(stock_pool, _date)
        base_current_value = DB_CONN['daily'].find_one(
            {'code': base, 'index': True, 'date': _date},
            projection={'close': True})['close']

        if i == 0: #首日为基准日
            base_begin_value = base_current_value
            portfolio_beging_capital = portfolio_current_capital
            print('初始资产: %10.2f' % (portfolio_beging_capital))
        else:
            print('收盘后，现金: %10.2f, 总资产: %10.2f' % (cash, portfolio_current_capital))
            df_profit.loc[_date] = {
                'net_value': round(portfolio_current_capital / portfolio_beging_capital, 2),
                'portfolio_profit': round(100 * (portfolio_current_capital - portfolio_beging_capital) / portfolio_beging_capital, 2),
                'base_profit': round(100 * (base_current_value - base_begin_value) / base_begin_value, 2)
            }
            #print(df_profit)

    print(df_profit)
    #drawdown = stock_util.compute_drawdown(df_profit['net_value'])
    #annual_profit, sharpe_ratio = stock_util.compute_sharpe_ratio(df_profit['net_value'])

    #print('回测结果 %s - %s，最大回撤：%7.3f, 夏普比率：%4.2f' % (begin_date, end_date, drawdown, sharpe_ratio))

    df_profit.plot(title='Backtest Result', y=['portfolio_profit', 'base_profit'], kind='line')
    plt.show()

def run(end_date):
    filename = r'E:\win7_data\Me\python_project\backtest\pool_macd.csv'
    stock_pool = pd.read_csv(filename,dtype={'code':str})
    stock_pool = stock_pool.drop_duplicates()
    stock_pool = stock_pool.set_index('code')
    #print(stock_pool)
    backtest('2018-10-15', end_date, stock_pool, '399006')

if __name__ == "__main__":
    end_date = '2018-11-02'
    run(end_date)
