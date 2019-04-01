"""
完成策略的回测，绘制以沪深300为基准的收益曲线，计算年化收益、最大回撤、夏普比率
主要的方法包括:
is_k_up_break_ma10：当日K线是否上穿10日均线
is_k_down_break_ma10：当日K线是否下穿10日均线
compare_close_2_ma_10：工具方法，某日收盘价和当日对应的10日均线的关系
backtest：回测主逻辑方法，从股票池获取股票后，按照每天的交易日一天天回测
compute_drawdown：计算最大回撤
compute_annual_profit：计算年化收益
compute_sharpe_ratio：计算夏普比率
"""

from pymongo import DESCENDING
import pandas as pd
import matplotlib.pyplot as plt
from stock_pool_strategy import stock_pool, find_out_stocks
from database import DB_CONN
import stock_util
import ipdb

# 持仓股代码列表
#holding_code_dict = {'300408':10000,'002454':20000}
#base = '399006'
def backtest(begin_date, end_date,holding_code_dict,base):
    """
    策略回测。结束后打印出收益曲线(沪深300基准)、年化收益、最大回撤、

    :param begin_date: 回测开始日期
    :param end_date: 回测结束日期
    """

    df_profit = pd.DataFrame(columns=['net_value','portfolio_profit', 'base_profit'])
    all_dates = stock_util.get_trading_dates(begin_date, end_date)
    holding_codes = list(holding_code_dict.keys())
    daily_value = dict(zip(holding_codes,range(-9999,len(holding_codes))))
    #print(daily_value)
    #ipdb.set_trace()

    cash = 0
    beging_capital = 0
    base_begin_value = 0

    # 按照日期一步步回测
    for i, _date in enumerate(all_dates):
        print('Backtest at %s.' % _date)

        if i == 0 : #首日为基准日
            pass

            #调整股票池
            #ajust_codes()

            #卖出信号及操作


            #买入出信号及操作


        # 计算总资产
        total_value = 0
        holding_daily_cursor = DB_CONN['daily'].find(
            {'code': {'$in': holding_codes}, 'date': _date},
            projection={'close': True, 'code': True, '_id':False})

        for holding_daily in holding_daily_cursor:
            code = holding_daily['code']
            holding_stock_volume = holding_code_dict[code]
            value = holding_daily['close'] * holding_stock_volume
            daily_value[code] = value

        for code in  holding_codes:
            value = daily_value[code]
            assert value > 0  # 起始日不能有停牌的股票！！！
            total_value += value
            #print('持仓: %s, %10.2f' % (code, value))

        total_capital = total_value + cash

        base_current_value = DB_CONN['daily'].find_one(
            {'code': base, 'index': True, 'date': _date},
            projection={'close': True})['close']

        if i == 0: #首日为基准日
            base_begin_value = base_current_value
            beging_capital = total_capital
        else:
            #print('收盘后，现金: %10.2f, 总资产: %10.2f' % (cash, total_capital))
            #print('初始资产: %10.2f' % (beging_capital))
            df_profit.loc[_date] = {
                'net_value': round(total_capital / beging_capital, 2),
                'portfolio_profit': round(100 * (total_capital - beging_capital) / beging_capital, 2),
                'base_profit': round(100 * (base_current_value - base_begin_value) / base_begin_value, 2)
            }
            #print(df_profit)
            #ipdb.set_trace()

    print(df_profit)
    drawdown = stock_util.compute_drawdown(df_profit['net_value'])
    annual_profit, sharpe_ratio = stock_util.compute_sharpe_ratio(df_profit['net_value'])

    print('回测结果 %s - %s，最大回撤：%7.3f, 夏普比率：%4.2f' %
          (begin_date, end_date, drawdown, sharpe_ratio))

    df_profit.plot(title='Backtest Result', y=['portfolio_profit', 'base_profit'], kind='line')
    plt.show()


if __name__ == "__main__":
    #backtest('2017-01-01', '2017-01-31', {'300408':10000,'002454':20000}, '399006')
    backtest('2018-07-01', '2018-09-12', {'300398':10000,'300192':20000}, '399006')
