# -*- coding:utf-8 -*-
"""
@author: lzy <liuzhy.20@pbcsf.tsinghua.edu.cn>
@file: Helper.py
@time:2021/11/27
Helper：因子平台的辅助库：1）交易日辅助模块，2）Benchmark辅助模块
"""

import pickle
import numpy as np
import pandas as pd
import os
import sys
import tushare as ts
import pymongo
sys.path.append('/home/lzy01/FactorBase/Code')
from mongodb_utils import *
client = pymongo.MongoClient(host='localhost', port=27017)


class TradeDate(object):

    __doc__ = """ 
    交易日处理模块，集成了：
        * 自动更新交易日数据库
        * 筛选查找交易日区间
        * 移动交易日
        * 生成月末（初），季度末（初），年末（初）交易日的功能
    """

    def __init__(
            self,
            sdt='2000-01-01',
            check_update=True
    ):

        # 取出目前储存的数据，比对确定是否需要更新
        if check_update:
            self.__update_dts()
        # 取出最新的trade date
        self.trade_dates = pd.DataFrame.from_records(
                client['basic_data']['Trade_Dates'].find(
                {'TRADE_DT': {"$gte": sdt}})
        ).sort_values('TRADE_DT')
        self.trade_dates['TRADE_DT'] = pd.to_datetime(self.trade_dates['TRADE_DT'])
        self.dates = sorted(self.trade_dates['TRADE_DT'].tolist())
        print('-' * 5 + ' TradeDate Initializing Finished ' + '-' * 5)
        return

    def __update_dts(self) -> None:
        """
        自动更新交易日数据库
        :return:
        """
        # 分别取出日期数据库和交易数据库中最新的日期进行比对：
        newest_dt = client['basic_data']['Trade_Dates'].find().sort([('TRADE_DT', -1)]).limit(1)[0]['TRADE_DT']
        newest_dt_price = client['basic_data']['Daily_return_with_cap'] \
            .find().sort([('TRADE_DT', -1)]).limit(1)[0]['TRADE_DT']

        if newest_dt >= newest_dt_price:
            print('No need for updating')
            return
        else:
            print('-' * 10 + ' Start Updating Trading Dates ' + '-' * 10)

            update_data = pd.DataFrame(

                pd.DataFrame.from_records(
                        client['basic_data']['Daily_return_with_cap'].find(
                        {'TRADE_DT': {"$gte": newest_dt}})
                )['TRADE_DT'].unique(),

                columns=['TRADE_DT']
            )

            update_data = update_data.iloc[1:, :]  # 由于取出的是大于等于日期数据库最后一天的数据，所以必须在添加时候把这一日期删除
            update_data['TRADE_DT'] = update_data['TRADE_DT'].apply(lambda x: pd.to_datetime(x))
            update_data['month'] = update_data['TRADE_DT'].apply(lambda x: x.month)
            update_data['year'] = update_data['TRADE_DT'].apply(lambda x: x.year)
            update_data['quarter'] = update_data['month'].apply(lambda x: self._get_quarter(x))

            insert_data = to_json_from_pandas(update_data[['TRADE_DT', 'month', 'year', 'quarter']])
            client['basic_data']['Trade_Dates'].insert_many(insert_data)

            print('-' * 10 + ' Updating Complete ' + '-' * 10)
            return

    def isTdt(
            self,
            tday
    ) -> bool:
        """
        判断输入的日期是否为trade dates
        :param tday:
        :return:
        """
        if isinstance(tday, pd.datetime):
            tday = pd.to_datetime(tday)
        else:
            tday = pd.to_datetime(str(tday))
        return tday in self.dates

    def offset(
            self,
            tday,
            n
    ) -> str:
        """
        挪动交易日，输入日期和挪动天数，返挪动后的交易日
        输入日期可以不是交易日，返回的是给定日期前一个交易日挪动后的结果
        :param tday: 目标日期，可以不是交易日 'YYYY-MM-DD'
        :param n:挪动幅度
        :return:挪动后的日期
        """
        if isinstance(tday, pd.datetime):
            tday = pd.to_datetime(tday)
        else:
            tday = pd.to_datetime(str(tday))

        lstday = max([i for i in self.dates if i <= tday])
        k = self.dates.index(lstday)
        tdayinx = k + n
        tdayinx = max(tdayinx, 0)
        return self.dates[tdayinx].strftime('%Y-%m-%d')

    def range(self, sdt, edt) -> list:
        """
        输出给定开始日期和结束日期内的交易日，两边都是闭区间
        :param sdt:开始日期 'YYYY-MM-DD'
        :param edt:结束日期 'YYYY-MM-DD'
        :return:
        """
        dates = self.trade_dates[
            (self.trade_dates['TRADE_DT'] >= pd.to_datetime(sdt))
            &
            (self.trade_dates['TRADE_DT'] <= pd.to_datetime(edt))]

        return [x.strftime('%Y-%m-%d') for x in dates['TRADE_DT'].to_list()]

    def range_with_freq(
            self,
            sdt: str,
            edt: str,
            freq='m',
            is_start=False,
    ) -> list:
        """
        取出sdt到edt之间所有月（m）或季（q）或年（y）的最后（开始）一天，以列表形式返回
        :param sdt: 开始日期 'YYYY-MM-DD'，可以不是交易日
        :param edt: 结束日期 'YYYY-MM-DD'，可以不是交易日
        :param freq: 频率，可以选择m，q，y （TODO：扩展到周频）
        :param is_start: 是否返回第一天，默认为False，返回最后一天。
        :return:
        """
        if freq not in ['m', 'q', 'y']:
            raise NotImplementedError('please enter the right fre: "m", "q", "y".')
        freq_dict = {"m": 'month', "q": 'quarter', "y": 'year'}

        trade_dates = self.trade_dates[(self.trade_dates['TRADE_DT'] >= pd.to_datetime(sdt)) &
                                       (self.trade_dates['TRADE_DT'] <= pd.to_datetime(edt))]
        freq_dates = trade_dates.sort_values('TRADE_DT', ascending=is_start).groupby([freq_dict[freq]]).first()

        return [x.strftime('%Y-%m-%d') for x in
                freq_dates.sort_values(freq_dict[freq], ascending=True)['TRADE_DT'].tolist()]

    @staticmethod
    def _get_quarter(month):
        if 3 < month <= 6:
            return 2
        elif 6 < month <= 9:
            return 3
        elif month > 9:
            return 4
        else:
            return 1


class BenchMark(object):

    __doc__ = """
    benchmark模块，集成了不同benchmark数据的调用和自动更新，目前有的数据有HS300，ZZ500，FullMarket VW
    """

    def __init__(self, check_update=True) -> None:

        # 目前支持的index
        self.benchmark_names = {'000300.SH': 'hs300', '000905.SH': 'zz500'}
        # 取出目前储存的数据，比对确定是否需要更新
        if check_update:
            self.__update_benchmark()
        print('-' * 5 + ' BenchMark Initializing Finished ' + '-' * 5)
        return

    # 将benchmark定义为callable对象，调用直接从数据库提取数据
    def __call__(self, sdt: str, edt: str, benchmark_name='full_market') -> pd.DataFrame:
        return fetch_data(sdt, edt, client['basic_data']['BenchMarks'],
                          time_query_key='TRADE_DT',
                          factor_ls=['TRADE_DT', benchmark_name]).iloc[:, 1:]

    def __update_benchmark(self) -> None:

        # 分别取出日期数据库和交易数据库中最新的日期进行比对：
        newest_dt = client['basic_data']['BenchMarks'].find().sort([('TRADE_DT', -1)]).limit(1)[0]['TRADE_DT']
        newest_dt_price = client['basic_data']['Daily_return_with_cap'] \
            .find().sort([('TRADE_DT', -1)]).limit(1)[0]['TRADE_DT']

        if newest_dt >= newest_dt_price:
            print('No need for updating BenchMark')
            return
        else:
            dts = TD.range(newest_dt, newest_dt_price)[1:]
            sdt, edt = dts[0], dts[-1]
            print('-' * 10 + ' Start Updating BenchMarks ' + '-' * 10)
            print('-' * 10 + f' range from {sdt} to {edt}' + '-' * 10)

            # -- step1 ：更新从ts上获取的数据
            sdt_ = pd.to_datetime(sdt).strftime('%Y%m%d')
            edt_ = pd.to_datetime(edt).strftime('%Y%m%d')
            ts_index = [self.get_index_from_ts(name, self.benchmark_names[name], sdt_, edt_)
                        for name in list(self.benchmark_names.keys())]

            # -- step2：更新从数据库上获取的数据
            fullmkt_index = self.get_fullMKT(sdt, edt)
            ts_index.append(fullmkt_index)

            # 插入数据
            for i in range(1,3):
                ts_index[0] = ts_index[0].merge(ts_index[i], on = ['TRADE_DT'], how = 'outer')
            insert_data = ts_index[0]
            self.insert = insert_data
            insert_data = to_json_from_pandas(insert_data)
            client['basic_data']['BenchMarks'].insert_many(insert_data)
            print('-' * 10 + ' Updating Complete ' + '-' * 10)
            return

    @staticmethod
    def get_index_from_ts(name: str, col: str, sdt: str, edt: str) -> pd.DataFrame:
        """
        从ts账号拉取index收益数据，返回dataframe
        【Note】:ts取数据区间是左右闭区间，而数据库fetch是左闭右开
        :param name:
        :param col:
        :param sdt: 'YYYYMMDD'!!
        :param edt:
        :return:
        """
        ts.set_token('dfb6e9f4f9a3db86c59a3a0f680a9bdc46ed1b5adbf1e354c7faa761')
        pro = ts.pro_api()

        df = pro.index_daily(ts_code=name, start_date=sdt, end_date=edt).loc[:, ['trade_date', 'pct_chg']]
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df['pct_chg'] = df['pct_chg'] / 100
        df.columns = ['TRADE_DT', col]
        df.sort_values('TRADE_DT', inplace=True)
        df.index = range(len(df))
        return df

    @staticmethod
    def get_fullMKT(sdt: str, edt: str) -> pd.DataFrame:
        # fetch data
        collection = client['basic_data']['Daily_return_with_cap']
        ret_data = fetch_data(sdt, edt, collection,
                              time_query_key='TRADE_DT',
                              factor_ls=['TRADE_DT', 'adj_pct_chg', 'TOT_SHR', 'S_DQ_CLOSE'])\
                   .dropna().iloc[:, 1:]
        # calculate VW MKT ret
        ret_data['cap'] = np.log(ret_data['TOT_SHR'] * ret_data['S_DQ_CLOSE'])
        cap = ret_data.groupby(['TRADE_DT'])[['cap']].sum().reset_index().rename(columns = {'cap': 'total_cap'})
        ret_data = ret_data.merge(cap, on='TRADE_DT', how='left')
        ret_data['weight'] = ret_data['cap'] / ret_data['total_cap']
        ret_data['ret_with_weight'] = ret_data['adj_pct_chg'] * ret_data['weight']
        mkt = ret_data.groupby('TRADE_DT')[['ret_with_weight']]\
              .sum().reset_index()\
              .rename(columns = {"ret_with_weight":'full_market'})
        return mkt

