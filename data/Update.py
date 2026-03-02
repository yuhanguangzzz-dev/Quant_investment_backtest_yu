import pandas as pd
import datetime as dt
import tushare as ts
import os
import Factor.CUti as CUti
from config import Token,TradeDay,Today,RAW_PATH,PROCESSED_PATH

pro = ts.pro_api(Token)

def update_basic_info():
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

    print('stock_basic data has been updated')
    df.to_csv(os.path.join(PROCESSED_PATH, 'stock_basic.csv'), index=False)
    return df

def update_stock_industry():
    df = pro.index_member_all(**{
        "l1_code": "",
        "l2_code": "",
        "l3_code": "",
        "is_new": "",
        "ts_code": "",
        "src": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "l1_code",
        "l1_name",
        "l2_code",
        "l2_name",
        "l3_code",
        "l3_name",
        "ts_code",
        "name",
        "in_date",
        "out_date",
        "is_new"
    ])

    print('stock_industry data has been updated')
    df.to_csv(os.path.join(PROCESSED_PATH, 'stock_industry.csv'), index=False)
    return df

def update_daily_index(startdate='20200101'):
    Index_df = []
    target_folder = PROCESSED_PATH
    index_type = ['000001.SH',
                  '000300.SH',
                  '000852.SH',
                  '000905.SH',
                  '932000.SZ',
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

    combined_matrix.to_csv(os.path.join(target_folder, 'index.csv'))

    print('index data has been updated')

    return combined_matrix

def update_daily_PV(StockList, TradeDayList, enddate):
    Days = []
    Stock_df = []
    target_folder = RAW_PATH
    datatype = ["open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount"]

    df = pd.read_csv(os.path.join(RAW_PATH, 'daily_close.csv'))

    lastday = df.iloc[-1, 0]

    for i in TradeDayList:
        if int(i) > int(lastday) and int(i) <= int(enddate):
            Days.append(i)
        else:
            continue

    print('更新天数为',Days)

    for date in Days:
        df = pro.daily(**{
            "ts_code": "",
            "trade_date": date,
            "start_date": "",
            "end_date": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount"
        ])

        Stock_df.append(df)

    if Days:
        stock_df = pd.concat(Stock_df, ignore_index=True)
        for data in datatype:
            filename = f'daily_{data}.csv'
            full_path = os.path.join(target_folder, filename)

            stock_martix = pd.read_csv(full_path, index_col=0)

            new_matrix = stock_df.pivot_table(index='trade_date',
                                              columns='ts_code',
                                              values=data,
                                              aggfunc='first')  # 确保每个股票每个日期只有一个值

            stock_matrix = pd.concat([stock_martix, new_matrix]).reindex(columns=StockList)

            stock_matrix.to_csv(full_path)
    else:
        print("The dataset has been updated")



def update_daily_moneyflow(StockList, TradeDayList, enddate):
    Days = []
    Stock_df = []
    target_folder = PROCESSED_PATH
    datatype = [
        "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
        "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
        "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
        "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
        "net_mf_vol", "net_mf_amount"
    ]

    df = pd.read_csv(os.path.join(PROCESSED_PATH, 'moneyflow_buy_elg_amount.csv'))

    lastday = df.iloc[-1, 0]

    for i in TradeDayList:
        if int(i) > int(lastday) and int(i) <= int(enddate):
            Days.append(i)
        else:
            continue

    if Days:
        print(Days)
    for date in Days:
        df = pro.moneyflow(**{
            "ts_code": "",
            "trade_date": date,
            "start_date": "",
            "end_date": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "buy_sm_vol",  # 小单买入量(手)
            "buy_sm_amount",  # 小单买入金额(万元)
            "sell_sm_vol",  # 小单卖出量(手)
            "sell_sm_amount",  # 小单卖出金额(万元)
            "buy_md_vol",  # 中单买入量(手)
            "buy_md_amount",  # 中单买入金额(万元)
            "sell_md_vol",  # 中单卖出量(手)
            "sell_md_amount",  # 中单卖出金额(万元)
            "buy_lg_vol",  # 大单买入量(手)
            "buy_lg_amount",  # 大单买入金额(万元)
            "sell_lg_vol",  # 大单卖出量(手)
            "sell_lg_amount",  # 大单卖出金额(万元)
            "buy_elg_vol",  # 特大单买入量(手)
            "buy_elg_amount",  # 特大单买入金额(万元)
            "sell_elg_vol",  # 特大单卖出量(手)
            "sell_elg_amount",  # 特大单卖出金额(万元)
            "net_mf_vol",  # 净流入量(手)
            "net_mf_amount"  # 净流入金额(万元)
        ])
        Stock_df.append(df)

    if Days:
        stock_df = pd.concat(Stock_df, ignore_index=True)
        for data in datatype:
            filename = f'moneyflow_{data}.csv'
            full_path = os.path.join(target_folder, filename)

            stock_martix = pd.read_csv(full_path, index_col=0)

            new_matrix = stock_df.pivot_table(index='trade_date',
                                              columns='ts_code',
                                              values=data,
                                              aggfunc='first')  # 确保每个股票每个日期只有一个值

            stock_matrix = pd.concat([stock_martix, new_matrix]).reindex(columns=StockList)

            stock_matrix.to_csv(full_path)

    else:
           print("The dataset has been updated")

def update_adjust_factor(StockList, TradeDayList, enddate):
    Days = []
    Stock_df = []
    target_folder = RAW_PATH
    datatype = [
    'adj_factor'
    ]

    df = pd.read_csv(os.path.join(RAW_PATH, 'daily_adj_factor.csv'))

    lastday = df.iloc[-1, 0]

    for i in TradeDayList:
        if int(i) > int(lastday) and int(i) <= int(enddate):
            Days.append(i)
        else:
            continue
    print(Days)
    for date in Days:
        df = pro.adj_factor(**{
            "ts_code": "",
            "trade_date": date,
            "start_date": "",
            "end_date": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "adj_factor"
        ])

        Stock_df.append(df)

    if Days:
        stock_df = pd.concat(Stock_df, ignore_index=True)
        for data in datatype:
            filename = f'daily_adj_factor.csv'
            full_path = os.path.join(target_folder, filename)

            stock_martix = pd.read_csv(full_path, index_col=0)

            new_matrix = stock_df.pivot_table(index='trade_date',
                                              columns='ts_code',
                                              values=data,
                                              aggfunc='first')  # 确保每个股票每个日期只有一个值

            stock_matrix = pd.concat([stock_martix, new_matrix]).reindex(columns=StockList)

            stock_matrix.to_csv(full_path)

    else:
        print("The dataset has been updated")

def update_daily_adjust_PV():
    '''data_list = ["open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount"]'''

    data_compute = ["open",
                "high",
                "low",
                "close",]

    data_keep = [
                "vol",
                "amount"]


    adj_factor = pd.read_csv(os.path.join(RAW_PATH, 'daily_adj_factor.csv'),index_col=0)
    last_adj_factor = adj_factor.iloc[-1,:]

    for data in data_compute:
        filename = f'daily_{data}.csv'
        Raw_PATH = os.path.join(RAW_PATH, filename)
        Processed_PATH = os.path.join(PROCESSED_PATH, filename)

        stock_matrix = pd.read_csv(Raw_PATH, index_col=0)
        stock_matrix = CUti.ts_multiply(adj_factor,stock_matrix).divide(last_adj_factor)

        stock_matrix.to_csv(Processed_PATH)

        if data == 'close':
            pct_change = CUti.ts_divide(stock_matrix, stock_matrix.shift(1))-1
            change = CUti.ts_minus(stock_matrix,stock_matrix.shift(1))
            stock_matrix.shift(1).to_csv(os.path.join(PROCESSED_PATH, 'daily_pre_close.csv'))
            pct_change.to_csv(os.path.join(PROCESSED_PATH, 'daily_pct_chg.csv'))
            change.to_csv(os.path.join(PROCESSED_PATH, 'daily_change.csv'))

    for data in data_keep:
        filename = f'daily_{data}.csv'
        Raw_PATH = os.path.join(RAW_PATH, filename)
        Processed_PATH = os.path.join(PROCESSED_PATH, filename)

        stock_matrix = pd.read_csv(Raw_PATH, index_col=0)
        stock_matrix.to_csv(Processed_PATH)


def update_All_data(TradeDayList, enddate=None):
    if enddate == None:
        enddate = dt.datetime.now().strftime('%Y%m%d')

    basic = update_basic_info()
    update_stock_industry()
    update_daily_index()

    stock_list= sorted(basic['ts_code'].tolist())

    update_daily_PV(stock_list, TradeDayList, enddate)
    update_daily_moneyflow(stock_list, TradeDayList, enddate)
    update_adjust_factor(stock_list, TradeDayList, enddate)
    update_daily_adjust_PV()





if __name__ == '__main__':
    update_daily_adjust_PV()