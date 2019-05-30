from multiprocessing.connection import Client
import pandas as pd
import numpy as np
import time
import os
import tushare as ts


file_hold_security = r'ini\hold_security.csv'


#{'ins':'open_portfolio', 'portfolio':'5G','code':'profit','cost':0,'num':0,'agent':'pingan'}
def open_portfolio(ins_dict):
    df = pd.read_csv(file_hold_security,dtype={'code':str})

    #验证此组合不存在
    df1 = df[df.agent == ins_dict['agent']]
    df1 = df1[df1.portfolio == ins_dict['portfolio']]
    if df1.empty:
        #组合开仓，增加一条profit记录
        ins_dict.pop('ins')
        df_dict = pd.DataFrame([ins_dict])
        df = df.append(df_dict,sort=False)
        df = df.loc[:,['portfolio','code','cost','num','agent']]
        df.to_csv(file_hold_security,index=False)
    else:
        print('组合已存在！！！')

#{'ins':'close_portfolio', 'portfolio':'5G','agent':'pingan'}
def close_portfolio(ins_dict):
    df = pd.read_csv(file_hold_security,dtype={'code':str})
    #获得组合相关的记录
    portfolio_index = df[(df.portfolio==ins_dict['portfolio']) & (df.agent==ins_dict['agent'])].index.tolist()
    if len(portfolio_index) == 1:
        cash01_row = df.loc[portfolio_index[0]]
        if cash01_row.at['code'] == 'cash01':
            #获得原来的cash, 并增加cash
            pre_cash_index = df[(df.portfolio=='cash') & (df.code=='cash01') & (df.agent==ins_dict['agent'])].index.tolist()
            df.loc[pre_cash_index[0],'cost'] += cash01_row.at['cost']

            #删除此记录
            df = df.drop(index=portfolio_index)
            df = df.loc[:,['portfolio','code','cost','num','agent']]
            df.to_csv(file_hold_security,index=False)
            print('portfolio_order: '+'close_portfolio success '+str(ins_dict))
        else:
            print('close_portfolio failed here1')
    else:
        print('close_portfolio failed here2')


def buy_sell(ins_dict):
    if ins_dict['ins'][:3] == 'buy':
        ins_dict['cost'] = ins_dict['cost']*1.0015
        df = pd.read_csv(file_hold_security,dtype={'code':str})
        #print(df)

        #获得原来的cash, 并减少cash
        pre_cash_index = df[(df.portfolio=='cash') & (df.agent==ins_dict['agent'])].index.tolist()
        df.loc[pre_cash_index[0],'cost'] -= ins_dict['cost']

        #获得原来的stock
        pre_stock_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code==ins_dict['code']) & (df.agent==ins_dict['agent'])].index.tolist()

        if len(pre_stock_index)>0:
            pre_row = df.loc[pre_stock_index[0]]
            pre_row.at['num']  += ins_dict['num']
            pre_row.at['cost'] += ins_dict['cost']

            #删除原来的记录
            df = df.drop(index=pre_stock_index)
            #补充更新后的记录
            df = df.append(pre_row, sort=False)
        else:
            #文件中不需要保存这两个字段
            if 'ins' in ins_dict:
                ins_dict.pop('ins')
            if 'price' in ins_dict:
                ins_dict.pop('price')
            #原来没有，新增一条记录
            df_dict = pd.DataFrame([ins_dict])
            df = df.append(df_dict, sort=False)
        #print(df)
        df = df.loc[:,['portfolio','code','cost','num','agent']]
        df.to_csv(file_hold_security,index=False)

    elif ins_dict['ins'][:4] == 'sell':
        df = pd.read_csv(file_hold_security,dtype={'code':str})

        #获得原来的cash, 并增加cash
        pre_cash_index = df[(df.portfolio=='cash') & (df.agent==ins_dict['agent'])].index.tolist()
        df.loc[pre_cash_index[0],'cost'] += ins_dict['cost']

        #获得原来的stock
        pre_stock_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code==ins_dict['code']) & (df.agent==ins_dict['agent'])].index.tolist()
        pre_row = df.loc[pre_stock_index[0]]

        #删除原来的记录
        df = df.drop(index=pre_stock_index)

        if pre_row['num'] > ins_dict['num']:
            pre_row['num']  -= ins_dict['num']
            pre_row['cost'] -= ins_dict['cost']
            #补充更新后的记录
            df = df.append(pre_row, sort=False)
        else:
            profit = ins_dict['cost'] - pre_row['cost']
            #获得profit
            profit_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code=='profit') & (df.agent==ins_dict['agent'])].index.tolist()
            df.loc[profit_index[0],'cost'] += profit

        #print(df)
        df = df.loc[:,['portfolio','code','cost','num','agent']]
        df.to_csv(file_hold_security,index=False)

    #to_log('portfolio_order: '+'update_portfolio success '+str(ins_dict))

#{'ins':'bonus_interest','portfolio':'advance','code':'123001','cost':200,'agent':'gtja'}
def bonus_interest(ins_dict):
    df = pd.read_csv(file_hold_security,dtype={'code':str})

    #获得原来的cash, 并增加cash
    pre_cash_index = df[(df.portfolio=='cash') & (df.agent==ins_dict['agent'])].index.tolist()
    df.loc[pre_cash_index[0],'cost'] += ins_dict['cost']

    #获得原来的stock
    pre_stock_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code==ins_dict['code']) & (df.agent==ins_dict['agent'])].index.tolist()
    df.loc[pre_stock_index[0],'cost'] -= ins_dict['cost']

    df = df.loc[:,['portfolio','code','cost','num','agent']]
    df.to_csv(file_hold_security,index=False)


    '''
    if ins_dict['ins'] == 'update':
        print('process update')
        df = pd.read_csv(file_hold_security,dtype={'code':str})
        df2 = pd.DataFrame(ins_dict['detail'])
        df1 = pd.merge(df,df2[['portfolio','code']],how='left',on=['portfolio','code'],indicator=True).query("_merge == 'left_only'").drop(columns=['_merge'])
        df3 = pd.concat([df1,df2])
        df = df3.loc[:,['portfolio','code','num','cost','agent']]
        #print(df)
        for i, row in df.iterrows():
            r.append(dict(row))
        df.to_csv('hold_security.csv',index=False)

    elif ins_dict['ins'] == 'delete':
        print('process delete')
        df = pd.read_csv('hold_security.csv',dtype={'code':str})
        df2 = pd.DataFrame(ins_dict['detail'])
        df1 = pd.merge(df,df2[['portfolio','code']],how='left',on=['portfolio','code'],indicator=True).query("_merge == 'left_only'").drop(columns=['_merge'])
        df = df1.loc[:,['portfolio','code','num','cost','agent']]
        #print(df)
        df.to_csv('hold_security.csv',index=False)

    #{'ins':'query_cash','agent':'pingan'}
    elif ins_dict['ins'] == 'query_cash':
        print('process query_cash')
        df = pd.read_csv('hold_security.csv')
        pre_cash_index = df[(df.portfolio=='share') & (df.code=='cash01') & (df.agent==ins_dict['agent'])].index.tolist()
        r.append(df.loc[pre_cash_index[0],'cost'])

    '''
#{'ins':'buy_order','portfolio':'original','code':'300408','num':1000,'cost':19999,'price':11.88,'agent':'pingan'}
def validate_order(ins_dict):
    r = False
    df = pd.read_csv(file_hold_security,dtype={'code':str})

    if ins_dict['ins'][:3] == 'buy':        #对于buy_order
        portfolio_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code=='cash01') & (df.agent==ins_dict['agent'])].index.tolist()
        if len(portfolio_index)>0:   #是否已开组合
            # cash_index = df[(df.portfolio=='cash') & (df.code=='cash01') & (df.agent==ins_dict['agent'])].index.tolist()
            # if len(cash_index)>0:  #券商现金是否足够
            #     row = df.loc[cash_index[0]]
            #     if row.at['cost'] > ins_dict['cost']*1.001:
            r = True
    elif ins_dict['ins'][:4] == 'sell':     #对于sell_order，是否已存在？
        #获得原来的stock
        stock_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code==ins_dict['code']) & (df.agent==ins_dict['agent'])].index.tolist()
        if len(stock_index)>0:
            row = df.loc[stock_index[0]]
            if row.at['num'] >= ins_dict['num']:
                r = True
    r = True    #暂不校验!
    return r


def deal_ins(ins_dict):
    r = []
    if ins_dict['ins'] == 'query_portfolio':
        to_log('portfolio_order: '+'process query_portfolio')
        df = pd.read_csv(file_hold_security)
        df = df[(df.portfolio==ins_dict['portfolio'])]
        for i, row in df.iterrows():
            r.append(dict(row))
    elif ins_dict['ins'] == 'query_all':
        #to_log('portfolio_order: '+'process query_all')
        df = pd.read_csv(file_hold_security)
        for i, row in df.iterrows():
            r.append(dict(row))
    elif ins_dict['ins'] == 'buy_order':
        if validate_order(ins_dict):
            buy_sell(ins_dict)
        else:
            to_log('portfolio_order: '+'validate buy_order error')
    elif ins_dict['ins'] == 'sell_order':
        if validate_order(ins_dict):
            buy_sell(ins_dict)
        else:
            to_log('portfolio_order: '+'validate sell_order error')
    elif ins_dict['ins'] == 'bonus_interest':
        bonus_interest(ins_dict)
    elif ins_dict['ins'] == 'open_portfolio':
        #to_log('portfolio_order: '+'process open_portfolio')
        open_portfolio(ins_dict)
    elif ins_dict['ins'] == 'close_portfolio':
        to_log('portfolio_order: '+'process close_portfolio')
        close_portfolio(ins_dict)

    return r

def trade_record():
    fname = input('file name:')
    strdate = input('date(20190201):')

    #备注ini文件
    ins = 'copy ' + file_hold_security + ' ' + file_hold_security + '_' + strdate
    os.system(ins)
    print(ins)
    #return

    file_trade_record = 'log\\trade_record\\' + fname + '.csv'
    df = pd.read_csv(file_trade_record,dtype='str',sep='&')
    df = df[df.date>=strdate]
    #print(df['date']);
    #print(df['ins']);
    #print(df.iloc[:,0]);
    #print(df.iloc[:,1]);
    #return

    for i,row in df.iterrows():
        ins_dict = str(row.ins)
        print(ins_dict)
        if ins_dict == 'nan':
            pass
        else:
            ins_dict = eval(ins_dict)
            # print(type(ins_dict))
            # print(ins_dict)
            deal_ins(ins_dict)

    # ins_dict = {'ins':'buy_order','portfolio':'5G',
    #             'code':'300136','num':800,'cost':20072,'agent':'pingan'}
    #ins_dict = {'ins':'sell_update','portfolio':'c_bond','code':'600980','num':3600,'cost':59696,'agent':'gtja'}
    #
    # ins_dict = {'ins':'sell_update','portfolio':'original',
    #             'code':'300999','num':1500,'cost':24450,'agent':'pingan'}
    #
    # ins_dict = {'ins':'open_portfolio', 'portfolio':'c_bond','code':'cash01','cost':0,'num':0,'agent':'pingan'}
    #
    # #ins_dict = {'ins':'close_portfolio', 'portfolio':'gas_oil','agent':'pingan'}
    # ins_dict = {'ins':'buy_update','portfolio':'c_bond',
    #             'code':'128020','num':20,'cost':1853,'agent':'pingan'}

def get_portfolio():
    ins_dict = {'ins':'query_all'}
    r = deal_ins(ins_dict)
    return pd.DataFrame(r)


def check_pingan():
    df_cash = pd.read_csv('ini\\pingan.csv',nrows=1,encoding='gbk') #资金
    df_stock = pd.read_csv('ini\\pingan.csv',dtype={'证券代码':'str'},skiprows=3,encoding='gbk') #持股
    df_stock = df_stock.loc[:,['证券代码','证券名称','股份余额']]
    df_stock.columns = ['code','证券名称','股份余额']

    df_p = get_portfolio()
    df_p = df_p.query('agent == "pingan"')
    print('pingan to update:\n',pd.merge(df_stock,df_p,how='left',on='code').query('股份余额!=num'))
    print('\npingan to delete:\n',pd.merge(df_p,df_stock,how='left',on='code',indicator=True).query('_merge=="left_only"'))
    #print(df_cash[['余额']])

def check_gtja():
    df_cash = pd.read_csv('ini\\gtja.csv',nrows=1,skiprows=5,encoding='gbk') #资金
    df_stock = pd.read_csv('ini\\gtja.csv',dtype={'证券代码':'str'},skiprows=8,encoding='gbk') #持股
    df_stock = df_stock.loc[:,['证券代码','证券名称','股票余额']]
    df_stock.columns = ['code','证券名称','股票余额']

    df_p = get_portfolio()
    df_p = df_p.query('agent == "gtja"')
    print('gtja to update:\n',pd.merge(df_stock,df_p,how='left',on='code').query('股票余额!=num'))
    print('\ngtja to delete:\n',pd.merge(df_p,df_stock,how='left',on='code',indicator=True).query('_merge=="left_only"'))
    #print(df_cash[['余额']])



import io
import pymongo
import zlib
conn = pymongo.MongoClient('localhost', 27017)
db = conn.stocks         #连接mydb数据库，没有则自动创建


def get_detail(rec):
    b = zlib.decompress(rec['detail'])
    s = b.decode()
    return s

def get_mongo_csv_df(rec):
    fss = io.StringIO(get_detail(rec))
    xd0 = pd.read_csv(fss,dtype={'code':'str'})
    #xd0 = pd.read_csv(fss,encoding='gbk')
    return xd0


def get_latest_tradingday():
    r = ''
    cols = db.idx
    tick = cols.find_one({'code':'000001'})
    if tick:
        df = get_mongo_csv_df(tick)
        r = df.iat[0,0]
    else:
        print('empty')
    return r

def get_day_all():
    cols = db.daily
    latest_tradingday = get_latest_tradingday()
    #print(latest_tradingday)
    tick = cols.find_one({'date':latest_tradingday})
    if tick:
        df = get_mongo_csv_df(tick)
        return df
    else:
        print('empty0')
        return None
df_all = get_day_all()

def get_stk_name(code):
    r = ''
    if df_all is not None:
        df1 = df_all[df_all.code == code]
        if df1.empty:
            if code[:2] == '12':
                r = 'AAA转债'
            pass
        else:
            r = df1.iat[0,1]
    else:
        print('empty1')
    return r

def get_stk_price(code):
    r = 0
    if df_all is not None:
        df1 = df_all[df_all.code == code]
        if df1.empty:
            if code[:2] == '12':
                r = 90
            if code[:3] == '519':
                r = 1
            pass
        else:
            r = df1.iat[0,3]
    else:
        print('empty2')
    return r

def get_stk_name_price(code):
    df = ts.get_realtime_quotes(code)
    if code[:3] == '519':
        return df.at[0,'name'], float(df.at[0,'price'])*100
    else:
        return df.at[0,'name'], float(df.at[0,'price'])

# 盘点组合
def pandian_p(idx,df):
    df1 = df.copy()
    df1 = df1.rename(columns = {'code':'代码','portfolio':'名称','num':'数量','cost':'成本','agent':'市值'})
    df1['市值'] = 0
    df1['名称'] = ''

    # 补充名称、市值
    cost, value = 0, 0
    for i,row in df1.iterrows():
        if row['代码'] in ('cash01'):
            df1.at[i,'市值'] = row['成本']
        elif row['代码'] in ('profit'):
            pass
        else:
            # df1.at[i,'名称'] = get_stk_name(row['代码'])
            # df1.at[i,'市值'] = row['数量'] * get_stk_price(row['代码'])
            df1.at[i,'名称'] , price = get_stk_name_price(row['代码'])
            df1.at[i,'市值'] = row['数量'] * price
            cost  += df1.at[i,'成本']
            value += df1.at[i,'市值']

    # 汇总
    df2 = pd.DataFrame([['     ',idx,0,cost,value]],columns=['代码','名称','数量','成本','市值'])
    df1 = df2.append(df1, sort=False)
    # 排序
    df1 = df1.sort_values(by='代码', ascending=False)
    #print(df1)
    return df1

# 盘点持仓
def pandian(agent=None):
    df_r = pd.DataFrame([['','现金','','',0],['','货基','','',0],['','持仓','',0,0],['','总计','','',0]],
    columns=['代码','名称','数量','成本','市值'])

    #agent = 'pingan'
    #agent = 'gtja'
    df1 = pd.read_csv(file_hold_security);#print(df1)
    df2 = df1[(df1.agent==agent)]
    #df2 = df1[(df1.agent==agent) & (~df1.portfolio.isin(['cash','money_fond']))]

    g = df2.groupby('portfolio').agg({'cost':np.sum})
    for idx in list(g.index):
        #print(g.loc[idx]['cost'])
        if idx == 'cash':
            df_r.iat[0,4] = g.loc[idx]['cost']
        elif idx == 'money_fond':
            df_r.iat[1,4] = g.loc[idx]['cost']
        else:
            df9 = pd.DataFrame([['','','','',''],],columns=['代码','名称','数量','成本','市值'])
            df_r = df_r.append(df9,sort=False)
            df9 = df2[df2.portfolio==idx]
            df9 = pandian_p(idx,df9)
            df_r = df_r.append(df9,sort=False)
            df_r.iat[2,4] += df9.iat[-1,4]
            df_r.iat[2,3] += df9.iat[-1,3];#print(df9)
    df_r.iat[3,4] = df_r.iat[0,4] + df_r.iat[1,4] + df_r.iat[2,4]

    #print(df_r)
    df_r.to_excel('e1.xlsx',index=False)

if __name__ == "__main__":
    trade_record()
    #check_pingan()
    #check_gtja()
    #pandian('gtja')
    pandian('pingan')

    # 有bug!!!
    # ins_dict = {'ins':'open_portfolio', 'portfolio':'c_bond','code':'cash01','cost':0,'num':0,'agent':'pingan'}
    # open_portfolio(ins_dict)
    # ins_dict = {'ins':'close_portfolio', 'portfolio':'wireless','agent':'pingan'}
    # close_portfolio(ins_dict)
