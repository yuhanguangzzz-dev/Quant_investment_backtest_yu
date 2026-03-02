import pandas as pd
import datetime as dt
import tushare as ts
import os

# time series 时序操作函数
def ts_add(matrix1, matrix2):
    if matrix1.shape != matrix2.shape:
        print('第一个矩阵的形状为', matrix1.shape, '第二个矩阵的形状为', matrix2.shape)
        raise ValueError("矩阵的形状必须相同")
    result_values = matrix1.values + matrix2.values
    return pd.DataFrame(result_values, index=matrix1.index, columns=matrix1.columns)

def ts_minus(matrix1, matrix2):
    if matrix1.shape != matrix2.shape:
        print('第一个矩阵的形状为', matrix1.shape, '第二个矩阵的形状为', matrix2.shape)
        raise ValueError("矩阵的形状必须相同")
    result_values = matrix1.values + matrix2.values
    return pd.DataFrame(result_values, index=matrix1.index, columns=matrix1.columns)

def ts_divide(matrix1, matrix2):
    if matrix1.shape != matrix2.shape:
        print('第一个矩阵的形状为',matrix1.shape, '第二个矩阵的形状为',matrix2.shape)
        raise ValueError("矩阵的形状必须相同")
    result_values = matrix1.values / matrix2.values
    return pd.DataFrame(result_values, index=matrix1.index, columns=matrix1.columns)

def ts_multiply(matrix1, matrix2):
    if matrix1.shape != matrix2.shape:
        print('第一个矩阵的形状为', matrix1.shape, '第二个矩阵的形状为', matrix2.shape)
        raise ValueError("矩阵的形状必须相同")
    result_values = matrix1.values * matrix2.values
    return pd.DataFrame(result_values, index=matrix1.index, columns=matrix1.columns)


def ts_sum(matrix, window):
    return matrix.rolling(window=window, min_periods=1).sum()

def ts_comproud(matrix, window):
    return matrix.rolling(window=window, min_periods=1).apply(lambda x: x.prod(), raw=True)

def ts_Mean(matrix,window):
    return matrix.rolling(window=window, min_periods=1).mean()

def ts_max(matrix, window):
    return matrix.rolling(window=window, min_periods=1).max()

def ts_min(matrix, window):
    return matrix.rolling(window=window, min_periods=1).min()

def ts_std(matrix, window):
    return matrix.rolling(window=window, min_periods=1).std()

def ts_corr(matrix1, matrix2, window):
    return matrix1.rolling(window=window, min_periods=1).corr(matrix2)




