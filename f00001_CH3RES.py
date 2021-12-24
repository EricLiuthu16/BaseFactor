# -*- coding:utf-8 -*-
"""
@author: lzy <liuzhy.20@pbcsf.tsinghua.edu.cn>
@file: CH3RES.py
@time:2021/12/12

"""
import time
import pandas as pd
import pymongo
import numpy as np
import statsmodels.api as sm

sys.path.append('/home/public/因子平台/BaseFiles')
from mongodb_utils import *
from Helper import TradeDate, BenchMark
from BaseFactor import BaseFactor
from multiprocessing import Pool

# 初始化数据库连接【NOTE：必须进行初始化，否则无法运行】
client = pymongo.MongoClient(host='localhost', port=27017)


class CH3RES(BaseFactor):
    __doc__ = """
    CH3 residual factor
    """

    def __init__(
            self,
            factor_name='f00001',
            factor_parameters={'lagTradeDays': 250, 'model': 'CH3'}
    ):

        # Initialize super class.
        super(CH3RES, self).__init__(factor_name=factor_name, factor_parameters=factor_parameters)
        self.lagTradeDays = self.factor_param['lagTradeDays']
        self.model_name = self.factor_param['model']

    def prepare_data(self, sdt, edt) -> None:
        """
        数据预处理: 获取eod及指数数据
        """

        # 多取一些数据做填充
        shifted_begin_date = self.TD.offset(sdt, -self.lagTradeDays)

        # 获取股票行情
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

        # --- 获取CH3因子收益
        self.CH3 = fetch_data(start_date=shifted_begin_date,
                              end_date=edt,
                              collection=client['basic_data']['CH3_Daily'],
                              time_query_key='TRADE_DT',
                              factor_ls=['TRADE_DT', 'mktrf', 'smb', 'vmg'])
        self.CH3 = self.CH3.set_index('TRADE_DT').sort_index(ascending=True)

        return

    def generate_factor(self, edt):
        """
        返回某一天因子的数值：shape = [1,n] where n is the num of tickers
        """
        begin_day = pd.to_datetime(self.TD.offset(edt, -self.lagTradeDays))
        edt = pd.to_datetime(edt)

        # 获取当天的ticker以及这些股票过去n天的收益、benchmark过去n天的收益
        EOD_edt = self.EOD.loc[begin_day:edt, :]
        ch3_edt = self.CH3.loc[begin_day:edt, :]

        # 筛选当天能计算的股票，要求数据量大于window的40%
        indicator = ~(
                (np.isnan(EOD_edt.iloc[-1, :])) |
                (np.nansum(np.isnan(EOD_edt), axis=0) >= int(self.lagTradeDays * 0.4))
        )
        EOD_edt = (EOD_edt.T[indicator.values]).T
        codes = list(EOD_edt.columns)

        # 开始计算
        residuals = []
        for code in codes:
            # 每只股票数据量与Benchmark数据量对应
            ret = EOD_edt[[code]].reset_index().merge(ch3_edt, on='TRADE_DT', how='inner').dropna()
            r, ch3 = ret[code].values, sm.add_constant(ret[['mktrf', 'smb', 'vmg']].values)

            model = sm.OLS(r, ch3)
            OLSresult = model.fit()
            res_ = r[-1] - np.nansum(
                ch3[-1, :].reshape(-1) *
                np.array(OLSresult.params).reshape(-1)
            )

            try:
                residuals.append(res_)
            except Exception:
                residuals.append(np.nan)

        residual_out = pd.Series(
            residuals, index=codes)

        return residual_out


if __name__ == '__main__':

    res = CH3RES()
    sdt = '2021-10-01'
    edt = '2021-11-01'

    # 测试
    res.prepare_data(sdt, edt)
    test = res.test_calculation(dt = '2021-10-12')

    # 计算并储存数据
    res.generate_factor_all(sdt, edt, process=5, nan_policy='keep')
    res.save()
