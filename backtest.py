import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import os
import matplotlib.pyplot as plt
import matplotlib
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import PROCESSED_PATH

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

def run_backtest_engine(signal_matrix, open_df, close_df, pre_close_df, pct_chg_df, amount_df, fixed_weight=0.04):
    """
    实盘级状态机回测引擎

    参数:
    signal_matrix: 交易信号矩阵 (1为持仓，0为空仓)
    open_df, close_df, pre_close_df, pct_chg_df, amount_df: 对应的价格与成交额数据
    fixed_weight: 每只股票的固定仓位，默认0.04 (即4%，最多持仓25只)

    返回:
    portfolio_returns: 每日投资组合的超额收益率序列
    stats: 回测统计信息（如涨跌停触及次数）
    """
    dates = signal_matrix.index

    # 初始化账户状态
    current_holdings = {}  # 记录当前持仓：{股票代码: 持仓权重}
    available_cash = 1.0  # 初始资金池为 100%

    # 记录收益和统计
    portfolio_returns = pd.Series(0.0, index=dates)
    limit_up_down_count = 0  # 统计整个回测期间碰到涨跌停的次数

    for i, date in enumerate(dates):
        if i == 0:
            continue  # 第一天没有前一日信号，跳过

        yesterday_date = dates[i - 1]

        # 获取当日的基础数据
        daily_open = open_df.loc[date]
        daily_close = close_df.loc[date]
        daily_pre_close = pre_close_df.loc[date]
        daily_pct_chg = pct_chg_df.loc[date]
        daily_amount = amount_df.loc[date]

        # 我们依据【昨天收盘后的信号】来决定【今天】的交易动作
        target_signals = signal_matrix.loc[yesterday_date]

        daily_pnl = 0.0  # 当日总收益
        daily_turnover = 0.0  # 当日换手率（用于算手续费）

        # 预先计算当日所有的涨跌停状态 (复用你之前的逻辑)
        # 这里统一用 decimal (如 0.1) 来判断
        is_limit_up = (daily_pct_chg > 0.09) & (daily_pct_chg < 0.11)  # 简化版，可根据板块细化
        is_limit_down = (daily_pct_chg > -0.11) & (daily_pct_chg < -0.09)
        limit_stocks = is_limit_up | is_limit_down

        # 1. 执行卖出 (原持仓中，今日信号为0的股票)
        sell_candidates = [s for s in current_holdings if target_signals.get(s, 0) == 0]
        for s in sell_candidates:
            # 假设按照开盘价卖出，收益计算区间：昨收 -> 今开
            # 注意：如果遇到一字跌停可能卖不出，这里为简化小白版本暂不阻断，仅记录
            if daily_pre_close.get(s, 0) > 0:
                ret_sell = (daily_open[s] / daily_pre_close[s]) - 1
            else:
                ret_sell = 0

            daily_pnl += current_holdings[s] * ret_sell
            daily_turnover += current_holdings[s]

            # 释放资金池并移除持仓
            available_cash += current_holdings[s]
            del current_holdings[s]

        # 2. 执行买入 (信号为1，且不在持仓中的股票)
        # 排除掉今天停牌/无成交额的股票
        valid_buys = [s for s in target_signals[target_signals == 1].index
                      if s not in current_holdings and daily_amount.get(s, 0) > 0]

        for s in valid_buys:
            # 只有资金池足够时才执行买入 (浮点数精度处理减去 1e-6)
            if available_cash >= fixed_weight - 1e-6:
                current_holdings[s] = fixed_weight
                available_cash -= fixed_weight

                # 按照开盘价买入，收益计算区间：今开 -> 今收
                if daily_open.get(s, 0) > 0:
                    ret_buy = (daily_close[s] / daily_open[s]) - 1
                else:
                    ret_buy = 0

                daily_pnl += fixed_weight * ret_buy
                daily_turnover += fixed_weight

        # 3. 处理继续持有的股票 (信号为1，且昨天已在持仓中)
        hold_candidates = [s for s in current_holdings if target_signals.get(s, 0) == 1]
        for s in hold_candidates:
            # 持有股票的收益区间：昨收 -> 今收 (即 pct_chg)
            ret_hold = daily_pct_chg.get(s, 0)
            if pd.isna(ret_hold):
                ret_hold = 0.0

            # 涨跌停降权处理：如果碰到涨跌停，计算收益时权重除以 1.5
            if limit_stocks.get(s, False):
                calc_weight = current_holdings[s] / 1.5
                limit_up_down_count += 1  # 统计触发次数
            else:
                calc_weight = current_holdings[s]

            daily_pnl += calc_weight * ret_hold

        # 4. 扣除交易手续费 (万1.5佣金 + 千0.5印花税估算)
        fee = (0.00015 * daily_turnover) + (0.0005 * (daily_turnover / 2))
        daily_pnl -= fee

        # 记录当日策略最终收益
        portfolio_returns[date] = daily_pnl

    # 临时插入这行代码诊断
    print("是否包含 NaN:", portfolio_returns.isnull().any())
    print("是否有极大/极小值:", portfolio_returns.min(), portfolio_returns.max())

    stats = {
        'limit_up_down_hit_count': limit_up_down_count
    }

    return portfolio_returns, stats


def analyze_and_plot_excess_return(portfolio_returns, engine_stats=None, index_id=None):
    """
    计算超额收益、风险指标并绘制累计超额收益曲线

    Parameters:
    portfolio_returns (pd.Series): 回测引擎输出的每日绝对收益率 (已扣除手续费)
    engine_stats (dict): 引擎自带的统计数据（如涨跌停次数）
    index_id (str): 基准指数代码，例如 '000300.SH'。如果不传，则计算绝对收益。

    Returns:
    pd.Series: 每日超额收益率序列
    dict: 评价指标字典
    """
    # 统一时间索引格式为字符串
    if not isinstance(portfolio_returns.index[0], str):
        portfolio_returns.index = portfolio_returns.index.astype(str)

    # 1. 获取基准指数收益率
    if index_id is None:
        index_ret = pd.Series(0.0, index=portfolio_returns.index)
        bench_name = "绝对收益"
    else:
        # 去除硬编码，使用 PROCESSED_PATH
        index_file = os.path.join(PROCESSED_PATH, 'index.csv')
        index_df = pd.read_csv(index_file, index_col=0)
        index_df.index = index_df.index.astype(str)

        # 截取与 portfolio_returns 相同时间段的指数收益
        index_ret = index_df[index_id].reindex(portfolio_returns.index).fillna(0.0)
        bench_name = f"相对于 {index_id} 超额"

    # 2. 计算超额收益率 (引擎出来的收益已经扣过 fee，所以这里直接减去基准即可)
    excess_returns = portfolio_returns - index_ret*0.01

    # 3. 计算评价指标
    trading_days = len(excess_returns)
    active_excess = excess_returns[portfolio_returns != 0]  # 只统计有持仓操作的日子

    if active_excess.empty:
        return excess_returns, {"错误": "该时间段内未产生任何有效交易信号"}

    # 累计超额收益率
    cumulative_excess = (1 + excess_returns).cumprod()
    total_excess_return = cumulative_excess.iloc[-1] - 1

    # 年化超额收益率
    annual_excess_return = (1 + total_excess_return) ** (252 / trading_days) - 1

    # 最大超额回撤 (即你的策略最多跑输基准多少)
    rolling_max = cumulative_excess.cummax()
    drawdown = (cumulative_excess / rolling_max) - 1
    max_drawdown = drawdown.min()

    # 信息比率 (Information Ratio，即超额收益的夏普比率)
    annual_volatility = excess_returns.std() * np.sqrt(252)
    if annual_volatility != 0:
        info_ratio = annual_excess_return / annual_volatility
    else:
        info_ratio = 0.0

    # 超额胜率 (跑赢基准的天数占比)
    win_days = active_excess[active_excess > 0]
    win_rate = len(win_days) / len(active_excess)

    # 汇总指标
    metrics = {
        "基准类型": bench_name,
        "累计超额收益": f"{total_excess_return * 100:.2f}%",
        "年化超额收益": f"{annual_excess_return * 100:.2f}%",
        "最大超额回撤": f"{max_drawdown * 100:.2f}%",
        "信息比率(超额夏普)": f"{info_ratio:.2f}",
        "跑赢基准胜率": f"{win_rate * 100:.2f}%",
        "活跃交易天数": f"{len(active_excess)} 天"
    }
    if engine_stats:
        metrics["涨跌停阻断次数"] = engine_stats.get('limit_up_down_hit_count', 0)

    # 4. 绘制超额收益曲线
    plt.figure(figsize=(12, 6))
    plt.plot(cumulative_excess.index, cumulative_excess.values, label=bench_name, color='red')
    plt.title(f'累计超额收益曲线 ({bench_name})', fontsize=15)
    plt.xlabel('日期', fontsize=12)
    plt.ylabel('累计净值 (1.0为起点)', fontsize=12)

    # 为了防止x轴日期过于密集，自动稀疏化刻度
    xticks = cumulative_excess.index[::max(1, len(cumulative_excess) // 10)]
    plt.xticks(xticks, rotation=45)

    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()

    # 商业化软件通常把图片存下来传给前端，而不是直接 show()
    # plt.savefig('excess_return_plot.png')

    return excess_returns, metrics, plt.gcf()