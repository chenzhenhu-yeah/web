
import pandas as pd
import numpy as np

import sys
sys.path.append(r'../../')
from quant_001.hu_talib import MA, MACD
from quant_001.stock_util import get_inx_mongo, get_stk_hfq_mongo, get_trading_dates_mongo
from quant_001.database import DB_CONN

class K:
    def __init__(self, s):
        self.open = s.open
        self.low  = s.low
        self.high = s.high
        self.close = s.close

    def is_yang(self, ):
        if self.close > self.open:
            return True
        else:
            return False

    def is_yin(self, ):
        if self.close < self.open:
            return True
        else:
            return False

    def entity_has(self, price):
        if price > self.open and price < self.close :
            return True
        else:
            return False

    def is_logo_yang(self, ):
        #print(self.close, self.open, self.high, self.low)
        if (self.high - self.low) > 0:
            if (self.close - self.open)/(self.high - self.low) > 0.67:
                return True
        else:
            return False

    def is_logo_yin(self, ):
        if (self.high - self.low) > 0:
            if (self.open - self.close)/(self.high - self.low) > 0.67:
                return True
        else:
            return False

    def chg_ratio(self, pre_price):
        return round((self.close - pre_price)/pre_price,4)

    def is_long_up_tail(self,):
        if self.is_yang():
            if (self.high - self.close)/(self.high - self.low) > 0.67:
                return True
        else:
            return False

    def is_long_down_tail(self,):
        if self.is_yin():
            if (self.close - self.low)/(self.high - self.low) > 0.67:
                return True
        else:
            return False

    def is_normal_up_tail(self,):
        if self.is_yang():
            if (self.high - self.close)/(self.high - self.low) > 0.5:
                return True
        else:
            return False

    def is_normal_down_tail(self,):
        if self.is_yin():
            if (self.close - self.low)/(self.high - self.low) > 0.5:
                return True
        else:
            return False

def up_3tricks(df):
    # 上升三法：趋势确认形态，多头趋势已形成，上升中继隔暴露继续做多意愿。
    # 1、处在上涨趋势中，5、10、30日均线呈多头排列；
    # 2、共5根K线，第1根和最后1根为阳线，其余为阴线；
    # 3、第1根涨5%以上，最后1根涨3%以上。
    # 4、阴线收盘不能低于第1根阳线最低价。
    # 5、筹码最好为扩散形态，且上方无筹码。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共31条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k1_close_30 = [df.iloc[i].close for i in range(1,31)]
    mean_close_30 = np.mean(k1_close_30);
    k1_close_10 = [df.iloc[i].close for i in range(1,11)]
    mean_close_10 = np.mean(k1_close_10);
    k1_close_5 = [df.iloc[i].close for i in range(1,6)]
    mean_close_5 = np.mean(k1_close_5);
    k0 = K(df.iloc[0])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[2])
    k3 = K(df.iloc[3])
    k4 = K(df.iloc[4])
    k5 = K(df.iloc[5])

    if mean_close_30<mean_close_10 and mean_close_10<mean_close_5:
        if k0.is_yang() and k1.is_yin() and k2.is_yin() and k3.is_yin() and k4.is_yang():
            if k4.chg_ratio(k5.close) and k0.chg_ratio(k1.close):
                if k3.close>k4.low and k2.close>k4.low and k1.close>k4.low:
                    r = True
    return r

def wash(df):
    # 横盘候假摔：
    # 20个交易日内振幅不超过15%
    # 输入：
    # df：columns包括['date','open','low','high','close'], 日期升序排列。
    # 返回：True、False

    r = False

    df1 = df[:20]
    low_list = [df1.iloc[i].low for i in range(0,20)]
    min_low = min(low_list)
    high_list = [df1.iloc[i].high for i in range(0,20)]
    max_high = max(high_list)


    #print(df)
    print(max_high/min_low)

    if max_high/min_low < 1.15:
        r = True

    return r

def rise(df):
    # 上台阶：
    # 10个交易日上升超15%
    # 输入：
    # df：columns包括['date','open','low','high','close'], 日期升序排列。
    # 返回：True、False

    r = False
    k0 = df.iloc[0]
    k10 = df.iloc[10]

    if k10.close/k0.close > 1.2:
        r = True

    return r


def k_pattern(df):
    '''
    输入：
        df：columns包括['date','open','low','high','close']
        记录按日期降序排列。
    '''
    r = ''

    df0 = df.sort_values('date')
    dates = list(df0['date'])    # 日期为升序
    d = None
    for i, date in enumerate(dates):
        if i > 10:
            df1 = df0[df0.date>=dates[i-10]]
            if rise(df1):
                d = date
                break
    print(d)

    if d is not None:
        df2 = df0[df0.date>=d]
        if len(df2) > 20:
            if wash(df2):
                #print('here')
                r = d
    return r


def signal_k_pattern(codes, begin_date, end_date):
    r = []
    for code in codes:
        df = get_stk_hfq_mongo(code, begin_date, end_date)
        date = k_pattern(df)
        if date != '':
            r.append([date,code])

    #print(r)
    return r

if __name__ == "__main__":
    begin_date = '2018-01-02'
    end_date  =  '2018-12-31'

    # df = pd.read_csv('stk_cyb.csv', dtype='str', encoding='gbk')
    # codes = list(df['code'])
    codes = ['002454','300433','002570','300136','002273','300073']
    #codes = ['002570']
    r = signal_k_pattern(codes, begin_date, end_date)
    df = pd.DataFrame(r, columns=['date','code'])
    df.to_csv('b1.csv', index=False)
