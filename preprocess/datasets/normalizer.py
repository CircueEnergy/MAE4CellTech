# -*- coding: utf-8 -*-


import sys
sys.path.insert(0, '../..')
sys.path = list(set(sys.path))

import numpy as np


class Normalizer:
    def __init__(self, dfs=None,params=None):
        """
        归一化
        :param dfs: list 包含每个dataframe
        :param variable_length: 是否是变长数据
        """

        res = []
        if dfs:
            res.extend(dfs)
            res = np.array(res)
            self.max_norm = 0
            self.min_norm = 0
            self.std = 0
            self.mean = 0
            self.compute_min_max(res)
        elif params:
            self.max_norm = np.array(params['max_norm'])
            self.min_norm = np.array(params['min_norm'])
            self.std = np.array(params['std'])
            self.mean = np.array(params['mean'])
        else:
            raise Exception("df list not specified")

    def compute_min_max(self, res):
        """
        计算最大最小均值与标准差
        """
        column_max_all = np.max(res, axis=1)
        column_min_all = np.min(res, axis=1)
        column_std_all = np.std(res, axis=1)
        column_mean_all = np.mean(res, axis=1)
        self.max_norm = np.max(column_max_all, axis=0)
        self.min_norm = np.min(column_min_all, axis=0)
        self.std = np.mean(column_std_all, axis=0)
        self.mean = np.mean(column_mean_all, axis=0)

    def norm(self, df):
        """
        归一化函数
        :param df: dataframe m * n
        :param norm_name: 归一化子类的前缀名
        :return: 调用子类的归一化函数的结果
        """
        return (df - self.mean) / np.maximum(np.maximum(1e-4, self.std), 0.1 * (self.max_norm - self.min_norm))


class Normalizer2:
    def __init__(self, params):
        self.mean = np.array(params['mean'])
        self.std = np.array(params['std'])

    def norm(self, df):
        return (df - self.mean) / (self.std + 1e-8)        









