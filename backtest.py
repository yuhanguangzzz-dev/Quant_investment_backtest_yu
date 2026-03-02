import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import os
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体
matplotlib.rcParams['axes.unicode_minus'] = False    # 解决负号显示问题

pro = ts.pro_api("c3532caffdb3fca514be0b7562e2f17d45bd1bed5242359c8ad1931a")
# 设置显示所有行
pd.set_option('display.max_rows', None)
# 设置显示所有列
pd.set_option('display.max_columns', None)
# 设置列宽
pd.set_option('display.max_colwidth', None)
# 设置显示宽度
pd.set_option('display.width', None)



def Port_Wgt_Get(Factor, TotelRet, Amount, startdate=None, enddate=None):
   """
   基于因子值进行选股和权重分配

   Parameters:
   Factor: 因子数据DataFrame，行索引为日期，列索引为股票代码
   TotelRet: 总收益数据DataFrame，与Factor行列结构一致
   Amount: 市值数据DataFrame，与Factor行列结构一致
   startdate: 起始日期
   enddate: 结束日期
   """

   # 根据起始和结束日期切割数据
   if startdate:
      Factor = Factor.loc[startdate:]
      TotelRet = TotelRet.loc[startdate:]
      Amount = Amount.loc[startdate:]

   if enddate:
      Factor = Factor.loc[:enddate]
      TotelRet = TotelRet.loc[:enddate]
      Amount = Amount.loc[:enddate]


   Factor = Factor.shift(1)

   # 不再检查索引和列的一致性，直接使用输入数据
   factor_df = Factor
   total_ret_df = TotelRet
   Amount_df = Amount

   # 初始化权重矩阵
   port_wgt = pd.DataFrame(0.0, index=factor_df.index, columns=factor_df.columns)

   # 遍历每个交易日
   for i, date in enumerate(factor_df.index):
      # 获取当日数据
      daily_factors = factor_df.loc[date]
      daily_returns = total_ret_df.loc[date]
      daily_Amount = Amount_df.loc[date]

      # 排除NaN值并排序
      valid_mask = ~daily_factors.isna()
      if valid_mask.sum() == 0:
         continue

      # 按因子值升序排序
      sorted_indices = daily_factors[valid_mask].sort_values(ascending=True).index

      # 计算选股数量
      n = len(sorted_indices)
      i_num = max(30, n // 4)  # 至少30只，或前25%

      # 选取因子值最大的股票（尾部是最大值）
      selected_stocks = sorted_indices[-i_num:]

      # 分配初始权重
      port_wgt.loc[date, selected_stocks] = 1.0

      # 识别涨跌停股票（降权处理）
      # 主板涨跌停 ±10%
      filter1 = (daily_returns > 0.09) & (daily_returns < 0.11) & \
                ((port_wgt.columns.astype(str).str[:3].astype(int) < 300) |
                 ((port_wgt.columns.astype(str).str[:3].astype(int) >= 600) & (
                         port_wgt.columns.astype(str).str[:3].astype(int) < 680)))

      filter2 = (daily_returns > -0.11) & (daily_returns < -0.09) & \
                ((port_wgt.columns.astype(str).str[:3].astype(int) < 300) |
                 ((port_wgt.columns.astype(str).str[:3].astype(int) >= 600) & (
                         port_wgt.columns.astype(str).str[:3].astype(int) < 680)))

      # 创业板/科创板涨跌停 ±20%
      filter3 = (daily_returns > 0.19) & (daily_returns < 0.21) & \
                (((port_wgt.columns.astype(str).str[:3].astype(int) >= 300) & (
                        port_wgt.columns.astype(str).str[:3].astype(int) < 400)) |
                 ((port_wgt.columns.astype(str).str[:3].astype(int) >= 688) & (
                         port_wgt.columns.astype(str).str[:3].astype(int) < 699)))

      filter4 = (daily_returns > -0.21) & (daily_returns < -0.19) & \
                (((port_wgt.columns.astype(str).str[:3].astype(int) >= 300) & (
                        port_wgt.columns.astype(str).str[:3].astype(int) < 400)) |
                 ((port_wgt.columns.astype(str).str[:3].astype(int) >= 688) & (
                         port_wgt.columns.astype(str).str[:3].astype(int) < 699)))

      # 识别无效股票（市值为NaN或0）
      filter5 = daily_Amount.isna() | (daily_Amount == 0)

      # 剔除无效股票
      port_wgt.loc[date, filter5[filter5].index] = np.nan

      # 涨跌停股票降权
      limit_up_down_filter = filter1 | filter2 | filter3 | filter4
      port_wgt.loc[date, limit_up_down_filter[limit_up_down_filter].index] = \
         port_wgt.loc[date, limit_up_down_filter[limit_up_down_filter].index] / 1.5

   # 权重标准化处理
   row_sums = port_wgt.sum(axis=1)
   filter_empty = row_sums == 0

   # 归一化权重
   for i, date in enumerate(port_wgt.index):
      if not filter_empty.iloc[i]:
         port_wgt.loc[date] = port_wgt.loc[date] / row_sums.iloc[i]
      else:
         port_wgt.loc[date] = 0

   # 处理剩余NaN值
   port_wgt = port_wgt.fillna(0)

   port_wgt = pd.DataFrame(port_wgt,index =Factor.index,columns =Factor.columns)

   # print(port_wgt)

   return port_wgt


def return_get(Port_Wgt, Stock_Return,startdate=None, enddate=None,Idx =None):
   if not isinstance(Port_Wgt.index[0], str):
      Port_Wgt.index = Port_Wgt.index.astype(str)
      Stock_Return.index = Stock_Return.index.astype(str)

   if startdate:
      Port_Wgt = Port_Wgt[startdate:]
      Stock_Return = Stock_Return[startdate:]

   if enddate:
      Port_Wgt = Port_Wgt[:enddate]
      Stock_Return = Stock_Return[:enddate]



   Total_Ret = Port_Wgt.multiply(Stock_Return).sum(axis=1)

   if Idx == None:
      # 创建一个与 port_wgt 行数相同的单列 DataFrame
      Index_Ret = pd.Series(0.0, index=Port_Wgt.index)

   else:
      Index_Ret = pd.read_csv(r'C:\Users\29967\Desktop\选股软件\data\processed\index.csv', index_col=0)
      Index_Ret = Index_Ret[Idx]

   Total_Ret.index = Port_Wgt.index.astype(str)
   Index_Ret.index = Stock_Return.index.astype(str)


   Turnover = Port_Wgt-Port_Wgt.shift(1)
   Turnover = Turnover.abs().sum(axis=1)
   Fee_Ret = (0.00015 * Turnover + 0.0005 * Turnover / 2)

   Excess_Ret = Total_Ret - Index_Ret- Fee_Ret
   return Excess_Ret

def plot_Excess_Ret(Factor, TotelRet, Amount, startdate=None, enddate=None,Idx=None):
   port_Wgt =Port_Wgt_Get(Factor, TotelRet, Amount, startdate, enddate)
   Excess_Ret = return_get(port_Wgt, TotelRet,startdate, enddate,Idx)
   # print(Excess_Ret)
   Cumulative_Excess_Return = (1 + Excess_Ret).cumprod()
   plt.figure(figsize=(12, 6))
   plt.plot(Cumulative_Excess_Return.index, Cumulative_Excess_Return.values)
   plt.title('累计超额收益曲线')
   plt.xlabel('日期')
   plt.ylabel('累计超额收益')
   plt.grid(True)
   plt.xticks(rotation=45)
   plt.tight_layout()
   plt.show()















