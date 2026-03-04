# ==========================================================
# Main.py: 系统统一调用中心 (后端 API 接口库)
# ==========================================================
import pandas as pd
import datetime as dt
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Token, TradeDay, Today, RAW_PATH, PROCESSED_PATH
from data.loaddata import StockData
from data.Update import (
    update_All_data, update_daily_PV, update_adjust_factor,
    update_daily_adjust_PV, update_basic_info, update_daily_index,
    update_stock_industry, update_daily_moneyflow
)
from strategy import strategy_low_vol_breakout
from backtest import run_backtest_engine, analyze_and_plot_excess_return

from data.repair import get_continuous_alignment_info, truncate_to_safe_date

def api_check_data_breakpoint():
    """获取第一个断点和需要修复的数据"""
    return get_continuous_alignment_info()

def api_execute_data_truncation(target_date):
    """仅执行截断操作"""
    try:
        files_count = truncate_to_safe_date(target_date)
        return True, f"已成功将 {files_count} 个底层数据文件截断至: {target_date}"
    except Exception as e:
        return False, f"截断失败: {str(e)}"

# ==========================================
# 接口 1：数据维护系统 (供前端“更新数据”按钮调用)
# ==========================================
def api_update_market_data(update_type="全部数据"):
    """
    灵活的数据更新接口，支持分类更新。
    """
    print("=" * 50)
    print(f">>> [系统提示] 正在执行数据更新任务: {update_type} ...")

    try:
        if update_type == "全部数据":
            update_All_data(TradeDayList=TradeDay, enddate=Today)  #
        elif update_type == "股票列表":
            update_basic_info()  #
            update_stock_industry()  #
        elif update_type == "指数数据":
            update_daily_index()  #
        else:
            # 针对需要 stock_list 的行情更新项
            basic_path = os.path.join(PROCESSED_PATH, 'stock_basic.csv')
            if not os.path.exists(basic_path):
                print(">>> 检测到缺失股票基础信息，正在自动拉取...")
                basic = update_basic_info()
            else:
                basic = pd.read_csv(basic_path)

            stock_list = sorted(basic['ts_code'].tolist())

            # 根据用户选择调用具体子函数
            if update_type == "量价数据":
                update_daily_PV(stock_list, TradeDay, Today)
            elif update_type == "复权因子":
                update_adjust_factor(stock_list, TradeDay, Today)
            elif update_type == "资金流向":
                update_daily_moneyflow(stock_list, TradeDay, Today)
            elif update_type == "复权量价计算":
                # 此步不依赖 API，仅进行本地复权计算
                update_daily_adjust_PV()

        print(f">>> [系统提示] {update_type} 更新已完成！")
        print("=" * 50)
        return True

    except Exception as e:
        print(f"❌ 数据更新失败: {e}")
        return False

# ==========================================
# 接口 2：回测执行系统 (供前端“开始回测”按钮调用)
# ==========================================
def api_run_backtest(strategy_name="low_vol", benchmark_id="000300.SH", max_pos_weight=0.04):
    """
    执行策略回测并返回评价报告
    """
    print("=" * 50)
    print(">>> [系统提示] 正在加载本地高速缓存数据...")
    data = StockData()
    data.load_all()

    # 提取量价矩阵
    # 加载复权数据 (来源于 DailyAdjust 命名空间)
    high_df = data.DailyAdjust.high
    low_df = data.DailyAdjust.low
    close_df = data.DailyAdjust.close
    pre_close_df = data.DailyAdjust.pre_close
    open_df = data.DailyAdjust.open

    # 涨跌幅和成交额的处理
    # 涨跌幅本身如果是基于复权计算的，可以直接从 DailyAdjust 取
    pct_chg_df = data.DailyAdjust.pct_chg

    # ⚠️ 成交额 (amount) 天然不需要复权，所以通常还是从原来的 Daily 里取
    amount_df = data.Daily.amount

    print(f">>> [系统提示] 正在运算策略信号: {strategy_name} ...")
    # 【未来扩展点】：这里可以写一个 if-else 或字典映射，根据前端传来的 strategy_name 调用不同的策略函数
    signal_matrix = strategy_low_vol_breakout(high_df, low_df, close_df, pre_close_df)

    print(">>> [系统提示] 正在启动状态机回测引擎...")
    abs_returns, engine_stats = run_backtest_engine(
        signal_matrix, open_df, close_df, pre_close_df, pct_chg_df, amount_df, fixed_weight=max_pos_weight
    )

    print(f">>> [系统提示] 正在生成相对于 {benchmark_id} 的超额评价报告...")
    excess_returns, report_metrics, fig = analyze_and_plot_excess_return(
        portfolio_returns=abs_returns,
        engine_stats=engine_stats,
        index_id=benchmark_id
    )

    print("\n" + "★" * 15 + " 回测体检报告 " + "★" * 15)
    for k, v in report_metrics.items():
        print(f"{k.rjust(12)} : {v}")
    print("★" * 42)

    # 将字典返回，方便未来前端框架提取并渲染成网页表格
    return excess_returns, report_metrics, fig


if __name__ == '__main__':
    # 你可以自由地组合调用它们

    # 1. 如果觉得数据不够新，把下面这行取消注释跑一下
    # api_update_market_data()

    # 2. 直接基于本地数据跑回测
    api_run_backtest(strategy_name="low_vol", benchmark_id="000300.SH")