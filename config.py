import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import os

# 路径配置
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')
RAW_PATH = os.path.join(DATA_PATH, 'raw')
PROCESSED_PATH = os.path.join(DATA_PATH, 'processed')

print ("当前路径为：", PROJECT_ROOT)

# API配置
Token ="c3532caffdb3fca514be0b7562e2f17d45bd1bed5242359c8ad1931a"

#全局时间
Today = dt.datetime.today().strftime('%Y%m%d')

#交易日历
TradeDay = ts.pro_api(Token).trade_cal(exchange='SSE', is_open='1',
                              start_date= '20200101',
                              end_date= Today,
                              fields='cal_date')
TradeDay = TradeDay["cal_date"].tolist()