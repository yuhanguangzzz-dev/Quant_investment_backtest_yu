import pandas as pd
import os
import sys

# 确保能找到 config 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import RAW_PATH


def check_index_mismatch():
    """
    检测复权因子与量价数据之间的索引不一致问题
    """
    adj_path = os.path.join(RAW_PATH, 'daily_adj_factor.csv')
    pv_path = os.path.join(RAW_PATH, 'daily_close.csv')  # 以收盘价作为量价数据代表

    # 1. 检查文件是否存在
    if not os.path.exists(adj_path) or not os.path.exists(pv_path):
        print("❌ 错误：检测文件不存在，请先执行数据更新。")
        return

    # 2. 读取索引 (只需读取第一列，提高速度)
    adj_df = pd.read_csv(adj_path, index_col=0, usecols=[0])
    pv_df = pd.read_csv(pv_path, index_col=0, usecols=[0])

    # 3. 转换为日期集合
    adj_dates = set(adj_df.index.astype(str))
    pv_dates = set(pv_df.index.astype(str))

    # 4. 找出差集
    extra_in_adj = adj_dates - pv_dates
    extra_in_pv = pv_dates - adj_dates

    # 5. 输出结果
    print("=" * 50)
    print("📊 数据一致性检测报告")
    print("-" * 50)

    if not extra_in_adj and not extra_in_pv:
        print("✅ 恭喜！复权因子与量价数据索引完全一致。")
    else:
        if extra_in_adj:
            print(f"⚠️ 复权因子多出的日期 ({len(extra_in_adj)}天):")
            print(sorted(list(extra_in_adj)))

        if extra_in_pv:
            print(f"⚠️ 量价数据多出的日期 ({len(extra_in_pv)}天):")
            print(sorted(list(extra_in_pv)))

    print("=" * 50)
    return sorted(list(extra_in_adj)), sorted(list(extra_in_pv))


def truncate_raw_data(target_date='20251205'):
    """
    强制截断所有原始量价数据和复权因子数据，删除指定日期之后的所有行。
    """
    # 1. 定义需要处理的原始文件名后缀
    # 包含：open, high, low, close, pre_close, change, pct_chg, vol, amount
    pv_types = ["open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]

    # 2. 汇总所有待处理的文件路径
    files_to_clean = [os.path.join(RAW_PATH, f'daily_{t}.csv') for t in pv_types]
    files_to_clean.append(os.path.join(RAW_PATH, 'daily_adj_factor.csv'))

    print(f"⚠️ 正在执行数据清理：将所有原始数据截断至 {target_date}...")

    count = 0
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            try:
                # 读取文件，不限制列，以日期为索引
                df = pd.read_csv(file_path, index_col=0)

                # 确保索引是字符串（Tushare日期格式如 '20251205'）
                df.index = df.index.astype(str)

                # 记录原始长度用于输出日志
                old_len = len(df)

                # 只保留小于等于目标日期的数据
                df = df[df.index <= str(target_date)]

                new_len = len(df)

                # 写回文件
                df.to_csv(file_path)
                print(f"✅ 已处理 {os.path.basename(file_path)}: 删除了 {old_len - new_len} 行。")
                count += 1
            except Exception as e:
                print(f"❌ 处理文件 {file_path} 时出错: {e}")
        else:
            print(f"ℹ️ 跳过不存在的文件: {os.path.basename(file_path)}")

    print(f"\n✨ 数据截断完成，共处理 {count} 个文件。")
    print("📢 请注意：截断原始数据后，请务必在前端点击“复权量价计算”以同步更新 PROCESSED 文件夹中的数据。")


if __name__ == "__main__":
    # 默认截断到 20251205
    check_index_mismatch()
    # truncate_raw_data('20251205')