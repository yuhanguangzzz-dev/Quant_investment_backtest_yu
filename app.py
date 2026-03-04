import streamlit as st
import pandas as pd
from Main import api_update_market_data, api_run_backtest,api_check_data_breakpoint,api_execute_data_truncation

# 页面配置
st.set_page_config(page_title="量化投资回测系统", layout="wide")

st.title("📈 量化投资回测控制台")
st.markdown("---")

# --- 侧边栏：参数配置 ---
st.sidebar.header("⚙️ 策略配置")

# 策略选择（目前只有低波动策略，后续可增加）
strategy_name = st.sidebar.selectbox(
    "选择策略",
    ["低波动突破策略 (low_vol)"]
)

# 基准指数选择
benchmark_id = st.sidebar.selectbox(
    "选择对比基准",
    ["000300.SH (沪深300)", "000905.SH (中证500)", "000852.SH (中证1000)"]
)
benchmark_code = benchmark_id.split(" ")[0]

# 仓位限制
max_pos = st.sidebar.slider("单只股票最大仓位 (%)", 1, 10, 4) / 100

# --- 主界面：数据更新 ---
st.subheader("数据管理")
col_sel, col_btn = st.columns([2, 1])

with col_sel:
    # 让用户选择更新的具体维度
    update_option = st.selectbox(
        "选择更新内容",
        ["全部数据", "量价数据", "复权因子", "复权量价计算", "股票列表", "指数数据", "资金流向"],
        index=0,
        help="建议初次使用选择'全部数据'。若仅需处理本地复权，请选择'复权量价计算'。"
    )

with col_btn:
    # 调整按钮位置使其对齐
    st.write("##")
    if st.button("🔄 执行更新", use_container_width=True):
        with st.spinner(f"正在更新 {update_option}..."):
            # 将用户选择的选项传给后端 API
            success = api_update_market_data(update_type=update_option)
            if success:
                st.success(f"{update_option} 更新任务成功！")
            else:
                st.error(f"{update_option} 更新失败，请检查终端日志。")

# app.py

st.markdown("---")
with st.expander("🛠️ 原始数据截断修复", expanded=False):
    st.write("用于扫描并清理因网络或中断导致的数据不对齐（寻找连续共同日期的第一个断点）。")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔍 扫描数据断点"):
            mismatches, safe_date = api_check_data_breakpoint()
            if not mismatches:
                st.success("✅ 数据完美对齐，无断点。")
            elif not safe_date:
                st.error("❌ 核心文件缺失，无法计算断点。")
            else:
                st.session_state['safe_date'] = safe_date  # 存入 session 供右侧使用
                st.warning(f"发现数据冲突！连续共有日期的最后一天为: **{safe_date}**")
                for name, dates in mismatches.items():
                    st.write(f"- {name} 需切除 {len(dates)} 天数据 (如 {dates[0]} 等)")

    with col2:
        # 获取刚才扫描出的安全日期，如果没有则默认为空
        default_date = st.session_state.get('safe_date', "")
        truncate_date = st.text_input("目标截断日期", value=default_date)

        if st.button("✂️ 仅执行截断", type="primary"):
            if not truncate_date:
                st.error("请先输入有效的截断日期。")
            else:
                with st.spinner(f"正在将原始数据切除至 {truncate_date}..."):
                    success, msg = api_execute_data_truncation(truncate_date)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)

# --- 主界面：运行回测 ---
st.subheader("策略回测")
if st.button("🚀 启动回测", type="primary"):
    with st.spinner("正在加载数据并运算信号..."):
        # 调用 Main.py 中的回测接口
        # 注意：这里会触发 backtest.py 中的绘图
        excess_returns, report_metrics, fig = api_run_backtest(
            strategy_name="low_vol",
            benchmark_id=benchmark_code,
            max_pos_weight=max_pos
        )

        # 显示体检报告
        st.write("### 📊 回测体检报告")

        display_df = pd.DataFrame([report_metrics]).T.reset_index()
        display_df.columns = ['指标', '数值']
        st.table(display_df.astype(str))

        # 在网页上直接显示图表
        st.pyplot(fig)