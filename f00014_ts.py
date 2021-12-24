# -*- coding:utf-8 -*-
"""
@author: lzy <liuzhy.20@pbcsf.tsinghua.edu.cn>
@file: ts.py
@time:2021/12/21

"""
import time
import pandas as pd
import pymongo
import numpy as np
import statsmodels.api as sm
from scipy.stats import skew

sys.path.append('/home/public/因子平台/BaseFiles')
from mongodb_utils import *
from Helper import TradeDate, BenchMark
from BaseFactor import BaseFactor
from multiprocessing import Pool

# 初始化数据库连接【NOTE：必须进行初始化，否则无法运行】
client = pymongo.MongoClient(host='localhost', port=27017)


class TS(BaseFactor):
    __doc__ = """
    ts factor
    """

    def __init__(
            self,
            factor_name='f00014',
            factor_parameters={'lagTradeDays': 250}
    ):

        # Initialize super class.
        super(TS, self).__init__(factor_name=factor_name, factor_parameters=factor_parameters)
        self.lagTradeDays = self.factor_param['lagTradeDays']

    def prepare_data(self, sdt, edt) -> None:
        """
        数据预处理
        """

        # 多取一些数据做填充
        shifted_begin_date = self.TD.offset(sdt, -self.lagTradeDays)

        # 获取收益率数据
        self.EOD = fetch_data(start_date=shifted_begin_date,
                              end_date=edt,
                              collection=client['basic_data']['Daily_return_with_cap'],
                              time_query_key='TRADE_DT',
                              factor_ls=['TRADE_DT', 'S_INFO_WINDCODE', 'adj_pct_chg'])

        # --- 为了计算方便，变为矩阵储存
        self.EOD = self.EOD.iloc[:, 1:].set_index(['TRADE_DT', 'S_INFO_WINDCODE']).unstack()
        self.dates = list(self.EOD.index)
        self.codes = [x[1] for x in list(self.EOD.columns)]
        self.EOD.columns = self.codes

        return

    def generate_factor(self, edt):
        """
        返回某一天因子的数值：shape = [1,n] where n is the num of tickers
        """
        begin_day = pd.to_datetime(self.TD.offset(edt, -self.lagTradeDays))
        edt = pd.to_datetime(edt)

        # 获取当天的ticker以及这些股票过去n天的收益、benchmark过去n天的收益
        EOD_edt = self.EOD.loc[begin_day:edt, :]

        # 筛选当天能计算的股票，要求数据量大于window的40%
        indicator = ~(
                (np.isnan(EOD_edt.iloc[-1, :])) |
                (np.nansum(np.isnan(EOD_edt), axis=0) >= int(self.lagTradeDays * 0.4))
        )
        EOD_edt = (EOD_edt.T[indicator.values]).T
        codes = list(EOD_edt.columns)

        # 开始计算
        ts = skew(EOD_edt, axis=0, nan_policy='omit')
        out = pd.Series(ts, index=codes)

        return out


if __name__ == '__main__':

    ts = TS()
    sdt = '2021-10-01'
    edt = '2021-11-01'

    # 测试
    ts.prepare_data(sdt, edt)
    test = ts.test_calculation(dt = '2021-10-12')

    # 计算并储存数据
    ts.generate_factor_all(sdt, edt, process=5, nan_policy='keep')
    ts.save()
