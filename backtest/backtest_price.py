
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import ipdb

from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_001']


cash = 1E8
single_position = 2E6

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

    holding_daily_cursor = DB_CONN['daily_hfq'].find(
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
    drawdown = stock_util.compute_drawdown(df_profit['net_value'])
    annual_profit, sharpe_ratio = stock_util.compute_sharpe_ratio(df_profit['net_value'])

    print('回测结果 %s - %s，最大回撤：%7.3f, 夏普比率：%4.2f' % (begin_date, end_date, drawdown, sharpe_ratio))

    df_profit.plot(title='Backtest Result', y=['portfolio_profit', 'base_profit'], kind='line')
    plt.show()

def run(end_date):
    filename = r'E:\win7_data\Me\python_project\backtest\pool_price.csv'
    stock_pool = pd.read_csv(filename,dtype={'code':str})
    stock_pool = stock_pool.drop_duplicates()
    stock_pool = stock_pool.set_index('code')
    #print(stock_pool)
    backtest('2018-10-01', end_date, stock_pool, '399006')

if __name__ == "__main__":
    end_date = '2018-10-12'
    run(end_date)
