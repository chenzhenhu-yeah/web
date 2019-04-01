#  -*- coding: utf-8 -*-
import pandas as pd
from pymongo import ASCENDING, DESCENDING
from database import DB_CONN
from datetime import datetime, timedelta


def get_trading_dates_mongo(begin_date=None, end_date=None):
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
        one_year_ago = now - timedelta(days=730)
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

def get_inx_mongo(code, begin_date=None, end_date=None):
    """
    获取指定日期范围的指数K线数据，
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日，
    返回结果按照交易日列表降序排列

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: df
    """
    # 开始日期，默认今天向前的365个自然日
    now = datetime.now()
    if begin_date is None:
        one_year_ago = now - timedelta(days=730)
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    daily_cursor = DB_CONN.daily.find(
        {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': True},
        sort=[('date', DESCENDING)],
        projection={'date':True,'close':True,'open':True,
                    'high':True,'low':True,'volume':True, '_id': False})

    r = [[x['date'],x['close'],x['open'],x['high'],x['low'],x['volume']] for x in daily_cursor]
    return pd.DataFrame(r, columns=['date','close','open','high','low','volume'])

def get_stk_hfq_mongo(code, begin_date=None, end_date=None):
    """
    获取指定日期范围的股票K线数据，
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日，
    返回结果按照交易日列表降序排列

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: df
    """
    # 开始日期，默认今天向前的365个自然日
    now = datetime.now()
    if begin_date is None:
        one_year_ago = now - timedelta(days=730)
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    daily_cursor = DB_CONN.daily_hfq.find(
        {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
        sort=[('date', DESCENDING)],
        projection={'date':True,'close':True,'open':True,
                    'high':True,'low':True,'volume':True, '_id': False})

    r = [[x['date'],x['close'],x['open'],x['high'],x['low'],x['volume']] for x in daily_cursor]
    return pd.DataFrame(r, columns=['date','close','open','high','low','volume'])

def get_stk_bfq_mongo(code, begin_date=None, end_date=None):
    """
    获取指定日期范围的股票K线数据，
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日，
    返回结果按照交易日列表降序排列

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: df
    """
    # 开始日期，默认今天向前的365个自然日
    now = datetime.now()
    if begin_date is None:
        one_year_ago = now - timedelta(days=730)
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    daily_cursor = DB_CONN.daily.find(
        {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
        sort=[('date', DESCENDING)],
        projection={'date':True,'close':True,'open':True,
                    'high':True,'low':True,'volume':True, '_id': False})

    r = [[x['date'],x['close'],x['open'],x['high'],x['low'],x['volume']] for x in daily_cursor]
    return pd.DataFrame(r, columns=['date','close','open','high','low','volume'])

def get_all_codes(date=None):
    """
    获取某个交易日的所有股票代码列表，如果没有指定日期，则从当前日期一直向前找，直到找到有
    数据的一天，返回的即是那个交易日的股票代码列表

    :param date: 日期
    :return: 股票代码列表
    """

    datetime_obj = datetime.now()
    if date is None:
        date = datetime_obj.strftime('%Y-%m-%d')

    codes = []
    while len(codes) == 0:
        code_cursor = DB_CONN.basic.find(
            {'date': date},
            projection={'code': True, '_id': False})

        codes = [x['code'] for x in code_cursor]

        datetime_obj = datetime_obj - timedelta(days=1)
        date = datetime_obj.strftime('%Y-%m-%d')

    return codes


if __name__ == '__main__':
    #df = get_stk_bfq_mongo('300408')
    df = get_inx_mongo('000001')
    print(df)
