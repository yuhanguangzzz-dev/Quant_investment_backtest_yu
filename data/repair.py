# data/validator.py
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import RAW_PATH, PROCESSED_PATH


def get_continuous_alignment_info():
    """
    寻找连续共同拥有的最晚时间（第一个断点所在）
    """
    # 核心修改：资金流向改为读取 PROCESSED_PATH 下的代表性文件
    check_files = {
        "量价行情(RAW)": os.path.join(RAW_PATH, 'daily_close.csv'),
        "复权因子(RAW)": os.path.join(RAW_PATH, 'daily_adj_factor.csv'),
        "资金流向(PROCESSED)": os.path.join(PROCESSED_PATH, 'moneyflow_buy_elg_amount.csv')
    }

    index_sets = {}
    all_dates = set()

    for name, path in check_files.items():
        if os.path.exists(path):
            df = pd.read_csv(path, index_col=0, usecols=[0])
            dates = set(df.index.astype(str))
            index_sets[name] = dates
            all_dates.update(dates)
        else:
            index_sets[name] = set()

    if not all_dates:
        return {}, None

    sorted_all_dates = sorted(list(all_dates))
    safe_date = None

    for date in sorted_all_dates:
        is_common = all(date in d_set for d_set in index_sets.values())
        if is_common:
            safe_date = date
        else:
            break

    mismatches = {}
    if safe_date:
        for name, d_set in index_sets.items():
            extra = {d for d in d_set if d > safe_date}
            if extra:
                mismatches[name] = sorted(list(extra))

    return mismatches, safe_date


def truncate_to_safe_date(target_date):
    """
    仅执行物理截断操作，涵盖所有量价、复权及 18 种资金流向数据
    """
    # 1. 整理需要截断的 RAW 文件 (量价 + 复权因子)
    pv_types = ["open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]
    all_files = [os.path.join(RAW_PATH, f'daily_{t}.csv') for t in pv_types]
    all_files.append(os.path.join(RAW_PATH, 'daily_adj_factor.csv'))

    # 2. 核心修改：整理需要截断的 PROCESSED 文件 (18个资金流向文件)
    mf_types = [
        "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
        "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
        "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
        "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
        "net_mf_vol", "net_mf_amount"
    ]
    all_files += [os.path.join(PROCESSED_PATH, f'moneyflow_{t}.csv') for t in mf_types]

    count = 0
    for path in all_files:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, index_col=0)
                df.index = df.index.astype(str)
                # 执行截断
                df = df[df.index <= str(target_date)]
                df.to_csv(path)
                count += 1
            except Exception as e:
                print(f"❌ 截断文件 {os.path.basename(path)} 失败: {e}")

    return count