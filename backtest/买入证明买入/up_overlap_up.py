
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


def up_overlap_up(df):
    # 多加多叠区：趋势转折形态，主力底部建仓信号。
    # 1、最新一根K线为标志性阳线；
    # 2、远端K线为标志性阳线，收盘为近期最低；
    # 3、中间不超过7根K线，且收盘不破位，总共多至9根K线；
    # 4、首尾两根阳线有叠区，叠区占比远端K线实体比例不少于2/3.

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共9条记录，第0条记录为最新K线，其余为之前的8根K线。
    # 返回：True、False

    r = False
    k1_low_list = [df.iloc[i].low for i in range(1,9)]
    min_low = min(k1_low_list);
    k0 = K(df.iloc[0])

    if k0.is_logo_yang() and k0.open>min_low:
        for i in range(2,9):
            ki = K(df.iloc[i])
            if ki.low == min_low: #第i根为起始k线
                if ki.is_logo_yang():
                    if k0.open>ki.open:
                        if (k0.close-k0.open)/(ki.close-ki.open)>0.33:
                            r = True
                            break
    return r


def k_pattern(df):
    '''
    输入：
        df：columns包括['date','open','low','high','close']
        记录按日期降序排列，第0条记录为当前分析日期。
    '''

    r = False
    # up_overlap_up
    df1 = df[:9]
    if up_overlap_up(df1):
        r = True

    return r


def signal_k_pattern(codes, begin_date, end_date):
    r = []
    for code in codes:
        df = get_stk_hfq_mongo(code, None, end_date)
        dates = list(df['date'])
        #print(dates)
        for date in dates:
            if date > begin_date:
                df1 = df[df.date<=date]
                if k_pattern(df1):
                    r.append([date,code])
            else:
                break
    #print(r)
    return r

if __name__ == "__main__":
    begin_date = '2018-01-02'
    end_date  =  '2018-12-12'

    # df = pd.read_csv('stk_cyb.csv', dtype='str', encoding='gbk')
    # codes = list(df['code'])
    codes = ['002454','300433','002570','300136','002273','300073']
    r = signal_k_pattern(codes, begin_date, end_date)
    df = pd.DataFrame(r, columns=['date','code'])
    df.to_csv('b1.csv', index=False)
