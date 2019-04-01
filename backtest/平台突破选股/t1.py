import ipdb
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_001']



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

    daily_cursor = DB_CONN.daily.find({'code': '002454', 'index': False})


    #dates = [x['date'] for x in daily_cursor]
    for cursor in daily_cursor:
        print(cursor)
        ipdb.set_trace()

    return dates

begin_date = '2018-08-12'
end_date = '2018-09-12'

dates = get_trading_dates(begin_date,end_date)
