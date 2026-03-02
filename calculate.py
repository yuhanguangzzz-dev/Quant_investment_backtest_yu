import numpy as np
import pandas as pd
from scipy import stats
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from functools import lru_cache


class CUti:
    _stat_vars = {}

    @staticmethod
    def stat_var(var_names=None, data=None):
        """管理全局变量（如交易日、调整因子等）"""
        if var_names is None and data is None:
            return CUti._stat_vars.copy()

        if data is not None:
            if isinstance(var_names, str):
                CUti._stat_vars[var_names] = data
            elif isinstance(var_names, list):
                for name, value in zip(var_names, data):
                    CUti._stat_vars[name] = value
            return data

        return CUti._stat_vars.get(var_names, None)

    # ====================== 行业/板块计算 ======================
    @staticmethod
    def pn_sector_avg(data, label, universe, a_mkt_cap, method='mean'):
        """行业中性化处理（按行业分组计算）"""
        label = label.copy()
        label[~universe] = 0
        value = pd.DataFrame(np.nan, index=data.index, columns=data.columns)

        for sector in label[universe].unique():
            if sector == 0:
                continue

            mask = (label == sector) & universe
            sector_data = data[mask]

            if method.lower() == 'mean':
                sector_mean = sector_data.mean(axis=1)
            elif method.lower() == 'median':
                sector_mean = sector_data.median(axis=1)
            elif method.lower() == 'amktcap':
                weights = a_mkt_cap[mask]
                sector_mean = (sector_data * weights).sum(axis=1) / weights.sum(axis=1)

            value[mask] = sector_mean.values[:, None]

        return value

    # ====================== 分组计算 ======================
    @staticmethod
    def pn_group_norm(data, label):
        """组内标准化（按分组标签）"""
        value = pd.DataFrame(np.nan, index=data.index, columns=data.columns)

        for group in label.dropna().unique():
            mask = (label == group)
            group_data = data[mask]
            z_scores = group_data.apply(lambda x: (x - x.mean()) / x.std(), axis=1)
            value[mask] = z_scores

        return value

    @staticmethod
    def pn_group_neutral(data, label):
        """组内去均值（按分组标签）"""
        value = pd.DataFrame(np.nan, index=data.index, columns=data.columns)

        for group in label.dropna().unique():
            mask = (label == group)
            group_mean = data[mask].mean(axis=1)
            value[mask] = data[mask].sub(group_mean, axis=0)

        return value

    # ====================== 横截面计算 ======================
    @staticmethod
    def pn_rank(data):
        """横截面排名（0-1标准化）"""
        return data.rank(axis=1, pct=True)

    @staticmethod
    def pn_winsor(data, multiple=3):
        """横截面去极值（标准差法）"""

        def winsorize_series(s):
            std = s.std()
            mean = s.mean()
            upper = mean + multiple * std
            lower = mean - multiple * std
            return s.clip(lower, upper)

        return data.apply(winsorize_series, axis=1)

    @staticmethod
    def pn_cut(data, upper_bound=95, lower_bound=5):
        """横截面截断（百分位法）"""

        def cut_series(s):
            upper = s.quantile(upper_bound / 100)
            lower = s.quantile(lower_bound / 100)
            return s.clip(lower, upper)

        return data.apply(cut_series, axis=1)

    # ====================== 时间序列计算 ======================
    @staticmethod
    def ts_delay(data, n_periods):
        """时间序列滞后"""
        return data.shift(n_periods)

    @staticmethod
    def ts_delta(data, n_periods):
        """时间序列差分"""
        return data.diff(n_periods)

    @staticmethod
    def ts_mean(data, n_periods):
        """滚动均值"""
        return data.rolling(n_periods).mean()

    @staticmethod
    def ts_std(data, n_periods):
        """滚动标准差"""
        return data.rolling(n_periods).std()

    @staticmethod
    def ts_corr(data1, data2, n_periods):
        """滚动相关系数"""
        return data1.rolling(n_periods).corr(data2)

    @staticmethod
    def ts_cov(data1, data2, n_periods):
        """滚动协方差"""
        return data1.rolling(n_periods).cov(data2)

    @staticmethod
    def ts_rank(data, n_periods):
        """滚动排名（时间序列内）"""
        return data.rolling(n_periods).apply(lambda x: stats.rankdata(x)[-1] / len(x))

    @staticmethod
    def ts_decay_linear(data, n_periods):
        """线性衰减均值（加权平均）"""
        weights = np.arange(1, n_periods + 1)
        weights = weights / weights.sum()

        def linear_decay(series):
            return series.rolling(n_periods).apply(
                lambda x: np.dot(x[-n_periods:], weights))

        return data.apply(linear_decay)

    # ====================== 基础运算 ======================
    @staticmethod
    def signed_power(data, power):
        """带符号的幂运算"""
        return np.sign(data) * np.abs(data) ** power

    @staticmethod
    def if_then(condition, true_val, false_val):
        """条件选择"""
        return np.where(condition, true_val, false_val)

    # ====================== 高级功能 ======================
    @staticmethod
    def ts_regression_fit(y, x, n_periods):
        """滚动线性回归（返回R²、残差等）"""
        results = pd.DataFrame(index=y.index, columns=['r_squared', 'residual'])

        for i in range(n_periods, len(y)):
            window_y = y.iloc[i - n_periods:i]
            window_x = x.iloc[i - n_periods:i]

            valid = window_y.notna() & window_x.notna().all(axis=1)
            if valid.sum() < 5:  # 最小样本要求
                continue

            model = LinearRegression().fit(window_x[valid], window_y[valid])
            pred = model.predict(window_x[valid])
            ss_res = ((window_y[valid] - pred) ** 2).sum()
            ss_tot = ((window_y[valid] - window_y[valid].mean()) ** 2).sum()

            results.at[y.index[i], 'r_squared'] = 1 - ss_res / ss_tot
            results.at[y.index[i], 'residual'] = (window_y[valid] - pred).mean()

        return results

    @staticmethod
    def pn_cross_fit(y, x):
        """横截面回归（去因子影响）"""
        residuals = pd.DataFrame(np.nan, index=y.index, columns=y.columns)

        for date in y.index:
            y_data = y.loc[date]
            x_data = x.loc[date]

            valid = y_data.notna() & x_data.notna().all(axis=1)
            if valid.sum() < 10:  # 最小样本要求
                continue

            model = LinearRegression().fit(x_data[valid], y_data[valid])
            pred = model.predict(x_data[valid])
            residuals.loc[date, valid] = y_data[valid] - pred

        return residuals

    @staticmethod
    def ts_stat_level(data, n_periods, n_clusters):
        """时间序列聚类（K-means）"""
        labels = pd.DataFrame(np.nan, index=data.index, columns=data.columns)

        for i in range(n_periods, len(data)):
            window = data.iloc[i - n_periods:i]
            scaler = StandardScaler()
            scaled = scaler.fit_transform(window.T)

            # 移除全NaN列
            valid_cols = ~np.isnan(scaled).any(axis=0)
            if valid_cols.sum() < n_clusters * 2:
                continue

            kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(scaled[:, valid_cols])
            labels.iloc[i] = kmeans.labels_

        return labels

    # ====================== 工具函数 ======================
    @staticmethod
    def convert_trd_day(data):
        """获取交易日标志（从全局变量）"""
        trade_day = CUti.stat_var('TradeDay')
        if trade_day is None:
            return pd.Series(True, index=data.index)
        return trade_day

    @staticmethod
    def ts_fill_non_trading_day(data):
        """非交易日数据前向填充"""
        return data.ffill()