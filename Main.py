import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import os
import matplotlib.pyplot as plt
import backtest as BT
import Factor.CUti as CUti
import Factor.library as Lib
import data.loaddata as Load
import data.Update as Up
import config  # 假设config.py在项目根目录

Token = config.Token
TradeDay = config.TradeDay
Today = config.Today


'''设定全局回测的股票池，股票池如下：
                    ['000001.SH', 上证指数
                    '000300.SH', 沪深300
                '   000852.SH', 
                  '000905.SH', 
                  '932000.SZ', 创业板
                  '399107.SZ',
                  '399006.SZ']'''

Universe = '000001.SH'

#更新数据
# Up.update_All_data(TradeDay)

'''loading data（不需要读取的数据请注释掉）'''

basic_info = Load.BasicInfo()
stock_industry = Load.StockIndustry()
Index = Load.DailyIndex()
PV = Load.PV()
MoneyFlow = Load.DailyMoneyflow()
adjust_factor = Load.AdjustFactor()
daily_adjust_pv = Load.DailyAdjustPV()

basic_info.load()
stock_industry.load()
Index.load()
PV.load_all()
# MoneyFlow.load_all()
# adjust_factor.load()
# daily_adjust_pv.load_all()

factor1 = Lib.Factor('均线收敛因子','日频量价').KY91_JSXL(PV)

print(PV.pct_chg)


BT.plot_Excess_Ret(factor1, PV.pct_chg, PV.amount, startdate= '20210101', enddate=Today,Idx = '000001.SH')



