import pandas as pd
import datetime as dt
import tushare as ts
import os
import sys

# 动态将项目根目录加入环境变量，确保能顺利导入 config 和 Factor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import processor.CUti as CUti
from config import Token, TradeDay, Today, RAW_PATH, PROCESSED_PATH

pro = ts.pro_api(Token)


def update_basic_info():
    df = pro.stock_basic(**{
        "ts_code": "", "name": "", "exchange": "", "market": "", "is_hs": "",
        "list_status": "", "limit": "", "offset": ""
    }, fields=[
        "ts_code", "symbol", "name", "industry", "market",
        "area", "list_status", "exchange", "list_date"
    ])

    print('stock_basic data has been updated')
    df.to_csv(os.path.join(PROCESSED_PATH, 'stock_basic.csv'), index=False)
    return df


def update_stock_industry():
    df = pro.index_member_all(**{
        "l1_code": "", "l2_code": "", "l3_code": "", "is_new": "",
        "ts_code": "", "src": "", "limit": "", "offset": ""
    }, fields=[
        "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
        "ts_code", "name", "in_date", "out_date", "is_new"
    ])

    print('stock_industry data has been updated')
    df.to_csv(os.path.join(PROCESSED_PATH, 'stock_industry.csv'), index=False)
    return df


def update_daily_index(startdate='20200101'):
    Index_df = []
    index_type = ['000001.SH',
                  '000300.SH',
                  '000852.SH',
                  '000905.SH',
                  '932000.CSI',
                  '399107.SZ',
                  '399006.SZ']

    for index in index_type:
        df = pro.index_daily(**{
            "ts_code": str(index),
            "trade_date": "",
            "start_date": startdate,
            "end_date": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "pct_chg",
        ])

        Index_df.append(df.pivot_table(index='trade_date',
                                       columns='ts_code',
                                       values='pct_chg',
                                       aggfunc='first'))


    combined_matrix = pd.concat(Index_df, axis=1)

    combined_matrix.to_csv(os.path.join(PROCESSED_PATH, 'index.csv'))
    print('index data has been updated')
    return combined_matrix


def update_daily_PV(StockList, TradeDayList, enddate):
    Days = []
    Stock_df = []
    datatype = ["open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]

    # 增量更新逻辑
    close_file_path = os.path.join(RAW_PATH, 'daily_close.csv')
    if os.path.exists(close_file_path):
        print('exist')
        df = pd.read_csv(close_file_path)
        lastday = df.iloc[-1, 0]
    else:
        lastday = '20200101'  # 设定一个默认起始日

    for i in TradeDayList:
        if int(i) > int(lastday) and int(i) <= int(enddate):
            Days.append(i)

    print('PV更新天数为', Days)

    for date in Days:
        df = pro.daily(**{
            "ts_code": "", "trade_date": date, "start_date": "", "end_date": "",
            "limit": "", "offset": ""
        }, fields=[
            "ts_code", "trade_date", "open", "high", "low", "close",
            "pre_close", "change", "pct_chg", "vol", "amount"
        ])
        Stock_df.append(df)

    if Days:
        stock_df = pd.concat(Stock_df, ignore_index=True)
        for data in datatype:
            filename = f'daily_{data}.csv'
            full_path = os.path.join(RAW_PATH, filename)

            new_matrix = stock_df.pivot_table(index='trade_date', columns='ts_code', values=data, aggfunc='first')

            if os.path.exists(full_path):
                stock_matrix = pd.read_csv(full_path, index_col=0)
                stock_matrix = pd.concat([stock_matrix, new_matrix]).reindex(columns=StockList)
            else:
                stock_matrix = new_matrix.reindex(columns=StockList)

            stock_matrix.to_csv(full_path)
    else:
        print("PV dataset is already up to date")


def update_daily_moneyflow(StockList, TradeDayList, enddate):
    Days = []
    Stock_df = []
    datatype = [
        "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
        "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
        "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
        "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
        "net_mf_vol", "net_mf_amount"
    ]

    mf_file_path = os.path.join(PROCESSED_PATH, 'moneyflow_buy_elg_amount.csv')
    if os.path.exists(mf_file_path):
        df = pd.read_csv(mf_file_path)
        lastday = df.iloc[-1, 0]
    else:
        lastday = '20200101'

    for i in TradeDayList:
        if int(i) > int(lastday) and int(i) <= int(enddate):
            Days.append(i)

    if Days:
        print('资金流向更新天数为', Days)

    for date in Days:
        df = pro.moneyflow(**{
            "ts_code": "", "trade_date": date, "start_date": "", "end_date": "",
            "limit": "", "offset": ""
        }, fields=["ts_code", "trade_date"] + datatype)
        Stock_df.append(df)

    if Days:
        stock_df = pd.concat(Stock_df, ignore_index=True)
        for data in datatype:
            filename = f'moneyflow_{data}.csv'
            full_path = os.path.join(PROCESSED_PATH, filename)

            new_matrix = stock_df.pivot_table(index='trade_date', columns='ts_code', values=data, aggfunc='first')

            if os.path.exists(full_path):
                stock_matrix = pd.read_csv(full_path, index_col=0)
                stock_matrix = pd.concat([stock_matrix, new_matrix]).reindex(columns=StockList)
            else:
                stock_matrix = new_matrix.reindex(columns=StockList)

            stock_matrix.to_csv(full_path)
    else:
        print("Moneyflow dataset is already up to date")


def update_adjust_factor(StockList, TradeDayList, enddate):
    Days = []
    Stock_df = []
    datatype = ['adj_factor']

    adj_file_path = os.path.join(PROCESSED_PATH, 'daily_adj_factor.csv')
    if os.path.exists(adj_file_path):
        df = pd.read_csv(adj_file_path)
        lastday = df.iloc[-1, 0]
    else:
        lastday = '20200101'

    for i in TradeDayList:
        if int(i) > int(lastday) and int(i) <= int(enddate):
            Days.append(i)

    if Days:
        print('复权因子更新天数为', Days)

    for date in Days:
        df = pro.adj_factor(**{
            "ts_code": "", "trade_date": date, "start_date": "", "end_date": "",
            "limit": "", "offset": ""
        }, fields=["ts_code", "trade_date", "adj_factor"])
        Stock_df.append(df)

    if Days:
        stock_df = pd.concat(Stock_df, ignore_index=True)
        for data in datatype:
            full_path = os.path.join(RAW_PATH, 'daily_adj_factor.csv')

            new_matrix = stock_df.pivot_table(index='trade_date', columns='ts_code', values=data, aggfunc='first')

            if os.path.exists(full_path):
                stock_matrix = pd.read_csv(full_path, index_col=0)
                stock_matrix = pd.concat([stock_matrix, new_matrix]).reindex(columns=StockList)
            else:
                stock_matrix = new_matrix.reindex(columns=StockList)

            stock_matrix.to_csv(full_path)
    else:
        print("Adjust factor dataset is already up to date")


def update_daily_adjust_PV():
    """
    严格遵守物理隔离：
    读取 RAW_PATH 的原始数据 -> 进行复权计算 -> 将计算结果写入 PROCESSED_PATH
    """
    data_compute = ["open", "high", "low", "close"]
    data_keep = ["vol", "amount"]

    # 从 RAW_PATH 读取复权因子
    adj_factor_path = os.path.join(RAW_PATH, 'daily_adj_factor.csv')
    adj_factor = pd.read_csv(adj_factor_path, index_col=0)
    last_adj_factor = adj_factor.iloc[-1, :]

    for data in data_compute:
        filename = f'daily_{data}.csv'
        raw_path = os.path.join(RAW_PATH, filename)

        # 1. 读取 RAW_PATH 中的原始数据
        stock_matrix = pd.read_csv(raw_path, index_col=0)

        # 2. 计算复权数据 (后复权基准)
        adj_stock_matrix = CUti.ts_multiply(adj_factor, stock_matrix).divide(last_adj_factor)

        # 3. 严格存入 PROCESSED_PATH，命名为 _adjust.csv 供 loaddata 使用
        processed_path = os.path.join(PROCESSED_PATH, f'daily_{data}_adjust.csv')
        adj_stock_matrix.to_csv(processed_path)

        # 基于复权后的收盘价计算真实的涨跌幅和涨跌额（这些属于处理后的衍生数据）
        if data == 'close':
            adj_pct_change = CUti.ts_divide(adj_stock_matrix, adj_stock_matrix.shift(1)) - 1
            adj_change = CUti.ts_minus(adj_stock_matrix, adj_stock_matrix.shift(1))

            # 将衍生数据存入 PROCESSED_PATH
            adj_stock_matrix.shift(1).to_csv(os.path.join(PROCESSED_PATH, 'daily_pre_close_adjust.csv'))
            adj_pct_change.to_csv(os.path.join(PROCESSED_PATH, 'daily_pct_chg_adjust.csv'))
            adj_change.to_csv(os.path.join(PROCESSED_PATH, 'daily_change_adjust.csv'))

    for data in data_keep:
        filename = f'daily_{data}.csv'
        raw_path = os.path.join(RAW_PATH, filename)

        # 读取 RAW_PATH 中的原始成交量和成交额
        stock_matrix = pd.read_csv(raw_path, index_col=0)

        # 量、额数据通常不参与价格复权计算，但为了配合 DailyAdjustPV 类的整体加载，
        # 我们将其视为 "处理完毕" 的数据，以 _adjust.csv 后缀存入 PROCESSED_PATH
        processed_path = os.path.join(PROCESSED_PATH, f'daily_{data}_adjust.csv')
        stock_matrix.to_csv(processed_path)


def update_All_data(TradeDayList, enddate=None):
    if enddate is None:
        enddate = dt.datetime.now().strftime('%Y%m%d')

    basic = update_basic_info()
    update_stock_industry()
    update_daily_index()

    stock_list = sorted(basic['ts_code'].tolist())

    update_daily_PV(stock_list, TradeDayList, enddate)
    update_daily_moneyflow(stock_list, TradeDayList, enddate)
    update_adjust_factor(stock_list, TradeDayList, enddate)
    update_daily_adjust_PV()


if __name__ == '__main__':
    # 你可以在这里直接运行增量更新测试
    update_daily_adjust_PV()