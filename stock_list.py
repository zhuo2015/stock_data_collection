# -*- coding: utf-8 -*-
import tushare as ts

print(ts.__version__)


def stock_list():
    # 获取沪深两市的股票代码，组成股票池
    df_stock_list = ts.get_stock_basics()
    df_stock_list_name = df_stock_list['name']
    return df_stock_list_name

stock_list = stock_list()
stock_list.to_csv('stock_list1.csv')

