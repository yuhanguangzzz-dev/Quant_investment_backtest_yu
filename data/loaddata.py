import pandas as pd
import os

class BasicInfo:
    """股票基本信息类"""

    def __init__(self):
        self.data = None

    def load(self):
        file_path = r'C:\Users\29967\Desktop\选股软件\data\processed\stock_basic.csv'
        if os.path.exists(file_path):
            self.data = pd.read_csv(file_path)
        else:
            print("Basic info file not found")
        return self.data


class StockIndustry:
    """股票行业分类类"""

    def __init__(self):
        self.data = None

    def load(self):
        file_path = r'C:\Users\29967\Desktop\选股软件\data\processed\stock_industry.csv'
        if os.path.exists(file_path):
            self.data = pd.read_csv(file_path)
        else:
            print("Stock industry file not found")
        return self.data


class DailyIndex:
    """指数日线行情类"""

    def __init__(self):
        self.data = None

    def load(self):
        file_path = r'C:\Users\29967\Desktop\选股软件\data\processed\index.csv'
        if os.path.exists(file_path):
            self.data = pd.read_csv(file_path, index_col=0)
        else:
            print("Daily index file not found")
        return self.data


class PV:
    """日频价格成交量数据类"""

    def __init__(self):
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.pre_close = None
        self.change = None
        self.pct_chg = None
        self.vol = None
        self.amount = None

    def load_all(self):
        data_types = ["open", "high", "low", "close", "pre_close",
                      "change", "pct_chg", "vol", "amount"]

        for data_type in data_types:
            file_path = f'C:/Users/29967/Desktop/选股软件/data/processed/daily_{data_type}.csv'
            if os.path.exists(file_path):
                setattr(self, data_type, pd.read_csv(file_path, index_col=0))
            else:
                print(f"Daily PV {data_type} file not found")
        return self


class DailyMoneyflow:
    """日频资金流向数据类"""

    def __init__(self):
        # 小单数据
        self.buy_sm_vol = None
        self.buy_sm_amount = None
        self.sell_sm_vol = None
        self.sell_sm_amount = None

        # 中单数据
        self.buy_md_vol = None
        self.buy_md_amount = None
        self.sell_md_vol = None
        self.sell_md_amount = None

        # 大单数据
        self.buy_lg_vol = None
        self.buy_lg_amount = None
        self.sell_lg_vol = None
        self.sell_lg_amount = None

        # 特大单数据
        self.buy_elg_vol = None
        self.buy_elg_amount = None
        self.sell_elg_vol = None
        self.sell_elg_amount = None

        # 净流入数据
        self.net_mf_vol = None
        self.net_mf_amount = None

    def load_all(self):
        data_types = [
            "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
            "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
            "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
            "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
            "net_mf_vol", "net_mf_amount"
        ]

        for data_type in data_types:
            file_path = f'C:/Users/29967/Desktop/选股软件/data/processed/moneyflow_{data_type}.csv'
            if os.path.exists(file_path):
                setattr(self, data_type, pd.read_csv(file_path, index_col=0))
            else:
                print(f"Moneyflow {data_type} file not found")
        return self


class AdjustFactor:
    """复权因子类"""

    def __init__(self):
        self.data = None

    def load(self):
        file_path = r'C:\Users\29967\Desktop\选股软件\data\processed\daily_adj_factor.csv'
        if os.path.exists(file_path):
            self.data = pd.read_csv(file_path, index_col=0)
        else:
            print("Adjust factor file not found")
        return self.data


class DailyAdjustPV:
    """复权后日频价格成交量数据类"""

    def __init__(self):
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.pre_close = None
        self.change = None
        self.pct_chg = None
        self.vol = None
        self.amount = None

    def load_all(self):
        data_types = ["open", "high", "low", "close", "pre_close",
                      "change", "pct_chg", "vol", "amount"]

        for data_type in data_types:
            file_path = f'C:/Users/29967/Desktop/选股软件/data/processed/daily_{data_type}_adjust.csv'
            if os.path.exists(file_path):
                setattr(self, data_type, pd.read_csv(file_path, index_col=0))
            else:
                print(f"Daily adjust PV {data_type} file not found")
        return self


class StockData:
    """主数据类，整合所有数据模块"""

    def __init__(self):
        self.BasicInfo = BasicInfo()
        self.StockIndustry = StockIndustry()
        self.DailyIndex = DailyIndex()
        self.Daily = PV()
        self.Moneyflow = DailyMoneyflow()
        self.AdjustFactor = AdjustFactor()
        self.DailyAdjust = DailyAdjustPV()

    def load_all(self):
        """加载所有数据"""
        self.BasicInfo.load()
        self.StockIndustry.load()
        self.DailyIndex.load()
        self.Daily.load_all()
        self.Moneyflow.load_all()
        self.AdjustFactor.load()
        self.DailyAdjust.load_all()
        return self


