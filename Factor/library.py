import pandas as pd
import datetime as dt
import tushare as ts
import os
import numpy as np
from . import CUti

class Factor:
    def __init__(self, factor_name, factor_type,Universe=None):
        self.factor_name = factor_name
        self.factor_type = factor_type
        if Universe:
            self.Universe = Universe
        else:
            self.Universe = None


    def KY91_JSXL(self,PV,IndLv = None):
        Amount = PV.amount
        ma5 = CUti.ts_Mean(Amount, 5)
        ma10 = CUti.ts_Mean(Amount, 10)
        ma20 = CUti.ts_Mean(Amount, 20)
        ma30 = CUti.ts_Mean(Amount, 30)
        ma60 = CUti.ts_Mean(Amount, 60)
        ma120 = CUti.ts_Mean(Amount, 120)

        Amount_matrix = np.stack([Amount, ma5, ma10, ma20, ma30, ma60, ma120], axis=2)

        # 计算因子值
        Factor = -np.log(1+np.nanstd(Amount_matrix, axis=2))
        Factor = pd.DataFrame(Factor, index=PV.amount.index, columns=PV.amount.columns)
        Factor = CUti.ts_Mean(Factor,20)

        if self.Universe:
            Factor[~self.Universe] = np.nan
        else:
            pass

        try:
            Factor = pd.DataFrame(Factor,index=PV.amount.index,columns=PV.amount.columns)
            print(Factor)
        except:
            print('数据处理失败')

        return Factor









