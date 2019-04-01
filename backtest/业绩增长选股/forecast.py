import tushare as ts
import pandas as pd
import numpy as np

df1 = ts.forecast_data(2018,3)
df1 = df1[df1.report_date>='2018-10-01']
df1 = df1.set_index('code')
print(df1.head(3))

df2 = pd.read_csv('sx_income_2018Q2.csv', dtype={'code':str})
df2 = df2.set_index('code')

df3 = pd.merge(df1,df2,how='inner')
print(df3)
df3.to_csv('a3.csv')
