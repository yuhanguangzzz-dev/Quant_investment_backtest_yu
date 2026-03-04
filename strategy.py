import pandas as pd
import numpy as np

import pandas as pd
import numpy as np


def strategy_low_vol_breakout(high_df, low_df, close_df, pre_close_df):
    """
    策略：低波动突破策略

    买入条件：
        1. 连续 5 日单日振幅小于 3%
        2. 连续 5 日内未出现过涨停或跌停 (剔除一字板等极端情况)
        3. 当日收盘价站上 20 日均线
    卖出条件：当日收盘价跌破 20 日均线
    持仓状态：0（空仓），1（持仓）。

    Parameters:
    high_df (pd.DataFrame): 最高价矩阵
    low_df (pd.DataFrame): 最低价矩阵
    close_df (pd.DataFrame): 收盘价矩阵
    pre_close_df (pd.DataFrame): 昨收价矩阵

    Returns:
    pd.DataFrame: 交易信号矩阵 (1为计划持仓，0为计划空仓)
    """

    # 1. 计算单日振幅：(最高价 - 最低价) / 昨收价
    daily_amp = (high_df - low_df) / pre_close_df
    cond_amp_daily = daily_amp < 0.03
    # 判断是否 "连续5日" 满足振幅小于3%
    cond_amp_5d = cond_amp_daily.rolling(window=5, min_periods=5).sum() == 5

    # ------------------ 新增逻辑开始 ------------------
    # 2. 计算每日涨跌幅 (使用小数表示，例如 0.10 代表 10%)
    daily_ret = (close_df - pre_close_df) / pre_close_df

    # 3. 判定单日是否涨跌停
    # (A股存在四舍五入的精度问题，通常涨跌幅达到 9.5% 即可视为触及涨跌停限制)
    is_limit = (daily_ret >= 0.095) | (daily_ret <= -0.095)

    # 4. 判断 "连续 5 日内" 涨跌停的天数是否为 0
    cond_no_limit_5d = is_limit.rolling(window=5, min_periods=5).sum() == 0
    # ------------------ 新增逻辑结束 ------------------

    # 5. 计算 20 日均线
    ma20 = close_df.rolling(window=20, min_periods=1).mean()

    # 6. 定义买入和卖出条件的布尔矩阵 (加入无涨跌停的条件)
    buy_condition = cond_amp_5d & cond_no_limit_5d & (close_df > ma20)
    sell_condition = close_df < ma20

    # 7. 状态机信号生成
    # 初始化全为 NaN 的矩阵
    signal_matrix = pd.DataFrame(np.nan, index=close_df.index, columns=close_df.columns)

    # 触发买入条件的那一天，状态标记为 1
    signal_matrix[buy_condition] = 1

    # 触发卖出条件的那一天，状态标记为 0
    signal_matrix[sell_condition] = 0

    # 使用 ffill() 向前填充状态（继承昨日的持仓状态）
    signal_matrix = signal_matrix.ffill()

    # 将初始或未触发过任何信号的 NaN 填为 0
    signal_matrix = signal_matrix.fillna(0)

    return signal_matrix