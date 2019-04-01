#  -*- coding: utf-8 -*-

from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
import tushare as ts
from datetime import datetime
from datetime import timedelta
import multiprocessing
import time
import pandas as pd

"""
从tushare获取日K数据，保存到本地的MongoDB数据库中
"""

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_001']
daily = DB_CONN['daily']
daily_hfq = DB_CONN['daily_hfq']


def daily_obj_2_doc(code, daily_obj):
    return {
        'code': code,
        'date': daily_obj['date'],
        'close': daily_obj['close'],
        'open': daily_obj['open'],
        'high': daily_obj['high'],
        'low': daily_obj['low'],
        'volume': daily_obj['volume']
    }

def show( code, index, hfq=False):
    r = []

    if hfq == False:
        r = daily.find({'code':code,'index':index},sort=[('date', ASCENDING)])
    if hfq == True:
        r = daily_hfq.find({'code':code,'index':index},sort=[('date', ASCENDING)])

    r = list(r)
    if len(r) == 0:
        print('no record')
    else:
        df1 = pd.DataFrame(r)
        date_list =  list(df1['date'])
        date_list.sort()
        s = set(date_list)
        print('list len :'+str(len(date_list)))
        print('set  len :'+str(len(s)))
        print(r[-3:])

def delete( code, index, hfq=False):

    if hfq == False:
        r = daily.delete_many({'code':code,'index':index})

    if hfq == True:
        r = daily_hfq.delete_many({'code':code,'index':index})

def save_data( code, df_daily, collection, extra_fields=None):
    """
    将从网上抓取的数据保存到本地MongoDB中

    :param code: 股票代码
    :param df_daily: 包含日线数据的DataFrame
    :param collection: 要保存的数据集
    :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
    """
    print(df_daily)
    for df_index in df_daily.index:
        daily_obj = df_daily.loc[df_index]
        doc = daily_obj_2_doc(code, daily_obj)

        if extra_fields is not None:
            doc.update(extra_fields)

        collection.insert_one(doc)
        #print('保存日线数据，代码：{}'.format(code))

def _get_latest_date( code, index, hfq=False):
    '''
    从mongodb中获取code数据最新的日期
    '''
    date_list = []
    latest_date = '2018-01-01'
    if hfq == False:
        date_list = daily.distinct('date',{'code':code,'index':index})

    if hfq == True:
        date_list = daily_hfq.distinct('date',{'code':code,'index':index})

    if len(date_list) > 0:
        date_list.sort()
        latest_date = date_list[-1]
        #set_trace()

    return latest_date

def _crawl_index_single( code, begin_date=None, end_date=None):
    """
    抓取指数的日线数据，并保存到本地数据数据库中
    抓取的日期范围从2018-01-01至今
    """

    # 设置默认的日期范围
    if begin_date is None:
        begin_date = _get_latest_date(code, index=True)
        begin_date = datetime.strptime(begin_date,'%Y-%m-%d')
        #print(type(begin_date),begin_date)
        begin_date += timedelta(days=1)
        #print(type(begin_date),begin_date)
        begin_date = begin_date.strftime('%Y-%m-%d')

    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    df_daily = ts.get_k_data(code, index=True, start=begin_date, end=end_date)
    save_data(code, df_daily, daily, {'index': True})

def crawl_index( begin_date=None, end_date=None):
    """
    抓取指数的日线数据，并保存到本地数据数据库中
    抓取的日期范围从2018-01-01至今
    """
    index_codes = ['000001', '000300', '399001', '399005', '399006']

    for code in index_codes:
        _crawl_index_single(code, begin_date, end_date)
        print(code)

def saver(q_saver):
    """
    将从网上抓取的数据保存到本地MongoDB中

    code, df_daily, collection, extra_fields=None
    :param code: 股票代码
    :param df_daily: 包含日线数据的DataFrame
    :param collection: 要保存的数据集
    :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
    """
    #print(df_daily)
    cycle = ''
    while True:
        r_list = q_saver.get(block = True);
        cycle = r_list[0];
        if cycle != 'complete':
            code = r_list[0];#print(code)
            df_daily = r_list[1];#print(df_daily)
            collection = r_list[2];#print(collection)
            extra_fields = r_list[3];#print(extra_fields)

            for df_index in df_daily.index:
                daily_obj = df_daily.loc[df_index]
                doc = daily_obj_2_doc(code, daily_obj)

                if extra_fields is not None:
                    doc.update(extra_fields)

                if collection == 'daily_hfq':
                    daily_hfq.insert_one(doc)
                else:
                    daily.insert_one(doc)
                #print('保存日线数据，代码：{}'.format(code))
        else:
            break
    print('saver complete!')

def _crawl_stock_single(q_saver, code, hfq, begin_date, end_date):
    # 设置默认的日期范围
    if begin_date is None:
        begin_date = _get_latest_date(code, False, hfq)
        begin_date = datetime.strptime(begin_date,'%Y-%m-%d')
        #print(type(begin_date),begin_date)
        begin_date += timedelta(days=1)
        #print(type(begin_date),begin_date)
        begin_date = begin_date.strftime('%Y-%m-%d')

    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    #print(type(begin_date),begin_date)
    #print(type(end_date),end_date)
    #ipdb.set_trace()

    beging_time = time.time()
    if hfq == False:
        df_daily = ts.get_k_data(code, autype=None, start=begin_date, end=end_date)
        mid_time = time.time()
        #print(code, ' net time:',mid_time-beging_time)
        q_saver.put([code, df_daily, 'daily', {'index': False}], block = True);
        #print(code, ' save time:',time.time()-mid_time)

    if hfq == True:
        df_daily_hfq = ts.get_k_data(code, autype='hfq', start=begin_date, end=end_date)
        mid_time = time.time()
        #print(code, ' hfq net time:',mid_time-beging_time)
        q_saver.put([code, df_daily_hfq, 'daily_hfq', {'index': False}], block = True)
        #print(code, ' hfq save time:',time.time()-mid_time)

def crawl_stock_bfq(q_saver, codes=[], begin_date=None, end_date=None):
    """
    获取所有股票从2018-01-01至今的K线数据（包括后复权和不复权三种），保存到数据库中
    """

    # 获取所有股票代码
    # stock_df = ts.get_stock_basics()
    # codes = list(stock_df.index)
    #codes = ['002454']

    i = 0
    stage = 0.01
    total = len(codes)
    print('bfq: ' + str(total))
    beging_time = time.time()
    for code in codes:
        i += 1
        if i/total > stage:
            print('bfq: ' + str(stage))
            stage += 0.1
        _crawl_stock_single(q_saver,code, False, begin_date, end_date)
    print('bfq use time:', time.time()-beging_time)

def crawl_stock_hfq(q_saver, codes=[], begin_date=None, end_date=None):
    """
    获取所有股票从2018-01-01至今的K线数据（包括后复权和不复权三种），保存到数据库中
    """


    i = 0
    stage = 0.01
    total = len(codes)
    print('hfq: ' + str(total))
    beging_time = time.time()
    for code in codes:
        i += 1
        if i/total > stage:
            print('hfq: ' + str(stage))
            stage += 0.1
        _crawl_stock_single(q_saver,code, True,  begin_date, end_date)
    print('hfq use time:', time.time()-beging_time)

def run():
    crawl_index() #通常情况下，不要提供参数！

    # stock_df = pd.read_csv('care_stocks.csv', dtype='str')
    # codes = list(stock_df.code)
    # print(codes)

    # 获取所有股票代码
    stock_df = ts.get_stock_basics()
    codes = list(stock_df.index)
    #codes = ['002454']

    q_saver = multiprocessing.Queue(10000)

    proc_save = multiprocessing.Process(target=saver, args=(q_saver,))
    proc_save.start()

    down_bfq = multiprocessing.Process(target=crawl_stock_bfq, args=(q_saver,codes))
    down_bfq.start()

    down_hfq = multiprocessing.Process(target=crawl_stock_hfq, args=(q_saver,codes))
    down_hfq.start()

    #proc_save.join()
    down_bfq.join()
    down_hfq.join()

    q_saver.put(['complete'])
    time.sleep(3)
    print('all complete')

if __name__ == '__main__':
    run()
