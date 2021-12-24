# -*- coding:utf-8 -*-
"""
@author: lzy <liuzhy.20@pbcsf.tsinghua.edu.cn>
@file: Beta.py
@time:2021/11/29
因子平台实例文件，实现了贝叶斯调整后的beta因子作为示例：
[1] Vasicek O A . A NOTE ON USING CROSS-SECTIONAL INFORMATION IN BAYESIAN ESTIMATION OF SECURITY BETAS[J].
The Journal of Finance, 1973.
"""
import time
import pandas as pd
import pymongo
import numpy as np
from scipy import stats

sys.path.append('/home/public/因子平台/BaseFiles')
from mongodb_utils import *
from Helper import TradeDate, BenchMark
from BaseFactor import BaseFactor
from multiprocessing import Pool

# 初始化数据库连接【NOTE：必须进行初始化，否则无法运行】
client = pymongo.MongoClient(host='localhost', port=27017)
# 初始化Benchmark辅助类
BM = BenchMark(check_update=False)


class BayesBeta(BaseFactor):
    __doc__ = """
    Bayes Beta Estimation
    """

    def __init__(
            self,
            factor_name='Beyes_Beta250',  # 自己取得因子名字
            factor_parameters={'lagTradeDays': 250, 'benchmark': 'full_market'}  # 定义一下这个因子单独需要的参数,如果没有就传一个空的进去
    ):

        # Initialize super class.
        super(BayesBeta, self).__init__(factor_name=factor_name, factor_parameters=factor_parameters)
        self.lagTradeDays = self.factor_param['lagTradeDays']
        self.benchmark = self.factor_param['benchmark']

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

        # 获取指数Benchmark
        self.BenchMark = BM(shifted_begin_date, edt, self.benchmark).set_index('TRADE_DT')

        return

    def generate_factor(self, edt):
        """
        返回某一天因子的数值：shape = [1,n] where n is the num of tickers
        """
        begin_day = pd.to_datetime(self.TD.offset(edt, -self.lagTradeDays))
        edt = pd.to_datetime(edt)

        # 获取当天的ticker以及这些股票过去n天的收益、benchmark过去n天的收益
        EOD_edt = self.EOD.loc[begin_day:edt, :]
        bm_edt = self.BenchMark.loc[begin_day:edt, :]

        # 筛选当天能计算的股票，要求数据量大于window的40%
        indicator = ~(
                (np.isnan(EOD_edt.iloc[-1, :])) |
                (np.nansum(np.isnan(EOD_edt), axis=0) >= int(self.lagTradeDays * 0.4))
        )
        EOD_edt = (EOD_edt.T[indicator.values]).T
        codes = list(EOD_edt.columns)

        # 开始计算
        beta = []
        stderr = []
        for code in codes:
            # 每只股票数据量与Benchmark数据量对应
            ret = EOD_edt[[code]].reset_index().merge(bm_edt, on='TRADE_DT', how='inner').dropna()
            r, bm = ret[code].values, ret[self.benchmark].values
            OLSresult = stats.linregress(r, bm)
            try:
                beta.append(OLSresult[0])
                stderr.append(OLSresult[-1])
            except Exception:
                beta.append(np.nan)
                stderr.append(np.nan)

        # Bayes调整
        beta_cal = pd.DataFrame({
            'beta': beta,
            'est_std': stderr,
        }, index=codes)

        std_cross = np.nanstd(beta_cal['beta'])
        mean_cross = np.nanmean(beta_cal['beta'])
        w = (
                (1 / beta_cal['est_std']) ** 2 /
                ((1 / beta_cal['est_std']) ** 2 + (1 / std_cross) ** 2)
        )
        beta = (beta_cal['beta'] * w + mean_cross * (1 - w)).values
        beta_df = pd.Series(beta, index=codes)

        return beta_df


if __name__ == '__main__':
    beta = BayesBeta()
    sdt = '2021-10-01'
    edt = '2021-11-01'

    # 测试
    beta.prepare_data(sdt, edt)
    beta.test_calculation(dt = '2021-10-12')

    # 计算并储存数据
    # beta.del_factor(sdt, '2021-11-13')
    beta.generate_factor_all(sdt, edt, process=5, nan_policy='keep')
    beta.save()
    test_data = fetch_data(sdt, edt, client['basic_data']['Beyes_Beta250'])

    # 更新数据
    beta = BayesBeta()
    beta.update_factor(process=5)
    test_data2 = fetch_data(sdt, '2021-11-12', client['basic_data']['Beyes_Beta250'])
    beta.update_factor(process=1)
