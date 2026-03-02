import pandas as pd
import datetime as dt
import tushare as ts
import os


# 设置显示所有列
pd.set_option('display.max_columns', None)

# 设置显示所有行
pd.set_option('display.max_rows', None)

# 设置每列的最大宽度为无限制
pd.set_option('display.max_colwidth', None)

# 设置显示宽度
pd.set_option('display.width', None)

pro = ts.pro_api("c3532caffdb3fca514be0b7562e2f17d45bd1bed5242359c8ad1931a")
target_folder = r'C:\Users\29967\Desktop\选股软件'

startdate = '20200101'
enddate  = dt.datetime.today().strftime('%Y%m%d')

TradeDay = pro.trade_cal(exchange='SSE', is_open='1',
                              start_date= startdate,
                              end_date= enddate,
                              fields='cal_date')

TradeDay = TradeDay["cal_date"].tolist()
print(TradeDay)

# basic_info = pd.read_csv(r'C:\Users\29967\Desktop\选股软件\stock_basic.csv')
tock_list = sorted(basic_info['ts_code'].tolist())
#
# flag = True
# stock_df = []
# for i in TradeDay:
#     df = pro.daily(**{
#         "ts_code": "",
#         "trade_date": i,
#         "start_date": "",
#         "end_date": "",
#         "limit": "",
#         "offset": ""
#     }, fields=[
#         "ts_code",
#         "trade_date",
#         "open",
#         "high",
#         "low",
#         "close",
#         "pre_close",
#         "change",
#         "pct_chg",
#         "vol",
#         "amount"
#     ])
#     stock_df.append(df)
#
#
#
# stock_df = pd.concat(stock_df,ignore_index = True)
# print(stock_df)
#
# datatype = ["open",
#         "high",
#         "low",
#         "close",
#         "pre_close",
#         "change",
#         "pct_chg",
#         "vol",
#         "amount"]
# for data in datatype:
#     matrix = stock_df.pivot_table(index='trade_date',
#                                   columns='ts_code',
#                                   values= data,
#                                   aggfunc='first')  # 确保每个股票每个日期只有一个值
#     filename = f'daily_{data}.csv'
#
#     full_path = os.path.join(target_folder,filename)
#
#     matrix.to_csv(full_path)

basic_info = pd.read_csv(r'C:\Users\29967\Desktop\选股软件\stock_basic.csv')
stock_list = sorted(basic_info['ts_code'].tolist())

flag = True
stock_df = []
for i in TradeDay:
    df = pro.adj_factor(**{
        "ts_code": "",
        "trade_date": i,
        "start_date": "",
        "end_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "adj_factor"
    ])

    stock_df.append(df)



stock_df = pd.concat(stock_df,ignore_index = True)
print(stock_df)

datatype = ['adj_factor']
for data in datatype:
    matrix = stock_df.pivot_table(index='trade_date',
                                  columns='ts_code',
                                  values= data,
                                  aggfunc='first')  # 确保每个股票每个日期只有一个值
    filename = f'daily_{data}.csv'

    full_path = os.path.join(target_folder,filename)

    matrix.to_csv(full_path)

# 存储所有资金流向数据的列表
# moneyflow_df_list = []
#
# # 循环获取每个交易日的资金流向数据
# for i, day in enumerate(TradeDay):
#     try:
#         # 调用资金流向接口
#         df_moneyflow = pro.moneyflow(**{
#             "ts_code": "",
#             "trade_date": day,
#             "start_date": '',
#             "end_date": '',
#             "limit": "",
#             "offset": ""
#         }, fields=[
#             "ts_code",
#             "trade_date",
#             "buy_sm_vol",  # 小单买入量(手)
#             "buy_sm_amount",  # 小单买入金额(万元)
#             "sell_sm_vol",  # 小单卖出量(手)
#             "sell_sm_amount",  # 小单卖出金额(万元)
#             "buy_md_vol",  # 中单买入量(手)
#             "buy_md_amount",  # 中单买入金额(万元)
#             "sell_md_vol",  # 中单卖出量(手)
#             "sell_md_amount",  # 中单卖出金额(万元)
#             "buy_lg_vol",  # 大单买入量(手)
#             "buy_lg_amount",  # 大单买入金额(万元)
#             "sell_lg_vol",  # 大单卖出量(手)
#             "sell_lg_amount",  # 大单卖出金额(万元)
#             "buy_elg_vol",  # 特大单买入量(手)
#             "buy_elg_amount",  # 特大单买入金额(万元)
#             "sell_elg_vol",  # 特大单卖出量(手)
#             "sell_elg_amount",  # 特大单卖出金额(万元)
#             "net_mf_vol",  # 净流入量(手)
#             "net_mf_amount"  # 净流入金额(万元)
#         ])
#
#         # 将获取的数据添加到列表中
#         if not df_moneyflow.empty:
#             moneyflow_df_list.append(df_moneyflow)
#             print(f"已获取交易日 {day} 的资金流向数据，共 {len(df_moneyflow)} 条记录")
#         else:
#             print(f"交易日 {day} 无资金流向数据")
#
#
#     except Exception as e:
#         print(f"获取交易日 {day} 的资金流向数据时出错: {e}")
#         continue
#
# # 合并所有资金流向数据
# if moneyflow_df_list:
#     moneyflow_all_df = pd.concat(moneyflow_df_list, ignore_index=True)
#     print(f"资金流向数据合并完成，总记录数: {len(moneyflow_all_df)}")
#
#     # 定义需要处理的资金流向数据类型
#     moneyflow_datatypes = [
#         "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
#         "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
#         "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
#         "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
#         "net_mf_vol", "net_mf_amount"
#     ]
#
#     # 对每种资金流向数据类型创建矩阵并保存
#     for data_type in moneyflow_datatypes:
#         try:
#             # 使用pivot_table创建数据矩阵
#             matrix = moneyflow_all_df.pivot_table(
#                 index='trade_date',
#                 columns='ts_code',
#                 values=data_type,
#                 aggfunc='first'  # 确保每个股票每个日期只有一个值
#             )
#
#             # 构建文件名和完整路径
#             filename = f'moneyflow_{data_type}.csv'
#             full_path = os.path.join(target_folder, filename)
#
#             # 保存为CSV文件
#             matrix.to_csv(full_path)
#             print(f"已保存 {data_type} 矩阵到 {full_path}，形状: {matrix.shape}")
#
#         except Exception as e:
#             print(f"处理 {data_type} 时出错: {e}")
#             continue
#
# else:
#     print("未获取到任何资金流向数据")
df = pro.stock_basic(**{
    "ts_code": "",
    "name": "",
    "exchange": "",
    "market": "",
    "is_hs": "",
    "list_status": "",
    "limit": "",
    "offset": ""
}, fields=[
    "ts_code",
    "symbol",
    "name",
    "industry",
    "market",
    "area",
    "list_status",
    "exchange",
    "list_date"
])

df.to_csv(r'C:\Users\29967\Desktop\选股软件\stock_basic.csv', index=False)
