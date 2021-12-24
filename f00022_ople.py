# -*- coding:utf-8 -*-
"""
@author: hlj
@file: OPLE.py
@time:2021/12/17

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


class OPLE(BaseFactor):
    __doc__ = """
    CH3 residual factor
    """

    def __init__(
            self,
            factor_name='f00022',
            factor_parameters={'lagTradeDays': 250}#为了距离sdt之前一期发布的财务报表，因此把数据获取提前一年（1998年之前的财报每年发布一次）
    ):

        # Initialize super class.
        super(OPLE, self).__init__(factor_name=factor_name, factor_parameters=factor_parameters)
        self.lagTradeDays = self.factor_param['lagTradeDays']

    def prepare_data(self, sdt, edt) -> None:
        """
        数据预处理: 获取eod及指数数据
        """

        # 多取一些数据做填充
        shifted_begin_date = self.TD.offset(sdt, -self.lagTradeDays)


        # 获取股票行情
        BALANCE = fetch_data(start_date=shifted_begin_date,
                              end_date=edt,
                              collection=client['basic_data']['asharebalancesheet_clean'],
                              time_query_key='TRADE_DT',
                              factor_ls=['TRADE_DT','REPORT_PERIOD', 'S_INFO_WINDCODE', 'TOT_SHRHLDR_EQY_INCL_MIN_INT','month_temp'])
        BALANCE=BALANCE[BALANCE['month_temp']==12].copy()#只取年报
        BALANCE['TOT_SHRHLDR_EQY_INCL_MIN_INT_shift1']=BALANCE.groupby('S_INFO_WINDCODE').TOT_SHRHLDR_EQY_INCL_MIN_INT.shift(1)


        INCOME=fetch_data(start_date=shifted_begin_date,
                              end_date=edt,
                              collection=client['basic_data']['ashareincome_discrete'],
                              time_query_key='TRADE_DT',
                              factor_ls=['TRADE_DT','REPORT_PERIOD','S_INFO_WINDCODE','OPER_PROFIT','month_temp'])
        INCOME = INCOME[INCOME['month_temp'] == 12].copy()  # 只取年报
        INCOME.drop(['TRADE_DT'],axis=1,inplace=True)#避免某些特殊情况两张表TRADE_DT不一致合并不上

        COMBINE=pd.merge(BALANCE,INCOME,on=['S_INFO_WINDCODE','REPORT_PERIOD'])
        COMBINE['REPORT_PERIOD'] = pd.to_datetime(COMBINE['REPORT_PERIOD'])
        COMBINE.sort_values(['S_INFO_WINDCODE','TRADE_DT','REPORT_PERIOD'],inplace=True)

        COMBINE.drop_duplicates(subset=['S_INFO_WINDCODE','TRADE_DT'],inplace=True,keep='last')#同一天发布则保留最新的一期


        #把年报数据向下填充到每一个交易日都具有
        tradingday_list=self.get_trading_days(sdt,edt)
        tradingday_list=pd.to_datetime(tradingday_list)
        dfs=[]
        for S_INFO_WINDCODE in COMBINE.S_INFO_WINDCODE.unique():
            df=COMBINE[COMBINE['S_INFO_WINDCODE']==S_INFO_WINDCODE].copy()
            df.index=df.TRADE_DT
            #print(df)
            df=df.reindex(tradingday_list,method='pad')
            dfs.append(df)

        self.EOD=pd.concat(dfs)

        return

    def generate_factor(self, edt):
        """
        返回某一天因子的数值：shape = [1,n] where n is the num of tickers
        """
        edt = pd.to_datetime(edt)

        # 获取当天的数据
        EOD_edt = self.EOD.loc[edt]
        EOD_edt['ople']=EOD_edt['OPER_PROFIT']/EOD_edt['TOT_SHRHLDR_EQY_INCL_MIN_INT_shift1']

        result_out = pd.Series(
            EOD_edt['ople'].tolist(), index=EOD_edt['S_INFO_WINDCODE'].tolist())

        return result_out


if __name__ == '__main__':

    ope = OPE()
    sdt = '2021-10-01'
    edt = '2021-11-01'

    # 测试
    ope.prepare_data(sdt, edt)
    test = ope.test_calculation(dt = '2021-10-12')

    # 计算并储存数据
    ope.generate_factor_all(sdt, edt, process=5, nan_policy='keep')
    ope.save()