# -*- coding:utf-8 -*-
"""
@author: lzy <liuzhy.20@pbcsf.tsinghua.edu.cn>
@file: BaseFactor.py
@time:2021/11/28
因子平台主文件，定义了因子基类，所有后续因子都建立在继承基类作为父类的基础上。
"""
import pickle
import time
import numpy as np
import pandas as pd
import os
import sys
import pymongo
from multiprocessing import Pool

sys.path.append('/home/lzy01/FactorBase/Code')
from mongodb_utils import *
from Helper import *

client = pymongo.MongoClient(host='localhost', port=27017)


# 定义因子基础类
class BaseFactor(object):
    __doc__ = """
    因子基类，用于因子的计算和存取
        * 当创建新的因子时，需要继承此类，实现prepare_data,和generate_factor方法，具体输出要求参见各个方法说明
        * 当更新旧的因子时，按照创建新因子时的参数实现实例，调用self.__update_factor()
        * 支持日频，月频，季频，年频因子的生成和维护，不同频率通过覆写self._get_trading_days()函数实现
    """

    def __init__(
            self,
            factor_name: str,
            factor_parameters: dict,
            save_db='basic_data',
            trade_date_update=False
    ) -> None:
        """
        :param factor_name:  (str)因子名，必须唯一
        :param tickers: 计算因子的投资品种范围
        :param factor_parameters: (dict)因子计算使用到的自定义参数
        :param save_db: 数据存储db的名称
        :param mongoclient: MongoDB 数据库
        """

        self.__factor_name = factor_name
        self.factor_param = factor_parameters
        # 储存计算的factor值
        self.__factor = []
        # 循环的trading_day
        self.__datetime = None
        # 储存存储因子的collection名称
        self.__save_db = save_db
        # 初始化交易日模块
        self.TD = TradeDate(check_update=trade_date_update)

    def get_factor_name(self):
        """
        获取因子唯一名称

        :return: (str)因子名
        """
        return self.__factor_name

    def get_trading_days(self, from_date, to_date) -> list:
        """
        获取计算因子的交易日历

        重写该函数可用于计算特定日期的因子， 如按月度进行计算
        * 月度、季度、年度频率的因子请使用：TD.range_with_freq(sdt,edt,freq,is_start)

        :param from_date: (str)起始时间
        :param to_date: (str)结束时间
        :return: (list)交易日
        """
        return self.TD.range(from_date, to_date)

    def prepare_data(self, sdt: str, edt: str):
        """
        .. note::
           必须实现prepare_data方法，用于一次性获取需要的数据,储存在类属性中。

        :param sdt: 原始数据起始日, YYYY-MM-DD
        :param edt: 原始数据结束日, YYYY-MM-DD
        """
        raise NotImplementedError

    def generate_factor(
            self,
            trading_day: str
    ) -> pd.Series:
        """
        .. note::
           必须实现generate_factor方法，用于计算某一天所有票因子并返回因子的值。
           返回一个Series，shape = [n,1] where n is the num of tickers, index是股票ticker

        :param trading_day: 交易日 YYYY-MM-DD
        """
        raise NotImplementedError

    def clear_factor(self, nan_policy='keep'):
        """
        对当天的因子进行清洗，主要有:
        1. 过滤掉无穷大和无穷小的值
        2. 过滤掉nan值
        .. todo::
           3. 过滤掉未上市的股票（未上市可能已经有财报发布，导致会出现一些值）
           4. 过滤掉已经退市的股票

        :return: 过滤后的因子值
        """
        if self.__factor is None or len(self.__factor) == 0:
            return

        # TODO 加入更多的filter
        factor_se = self.__factor.replace([np.inf, -np.inf], np.nan)
        try:
            odd = np.nansum(np.isnan(factor_se[self.__factor_name])) / len(self.__factor)
            print(f'Factor NaN pct = {round(odd * 100, 4)} %')
        except KeyError:
            print('Warning: factor data column name do not correspond to factor name, please check')
            pass

        if nan_policy == 'drop':
            self.__factor = factor_se.dropna()
        elif nan_policy == 'keep':
            self.__factor = factor_se

    def get_daily_result(self, dt):
        """
        多线程计算辅助函数，用于获取每天的因子值，主体是.generate_factor()
        :param dt:
        :return:
        """
        print(f' >>> {dt} {self.__factor_name} calculation begin')
        t0 = time.time()
        df = self.generate_factor(dt).to_frame()
        df['S_INFO_WINDCODE'] = df.index
        df['TRADE_DT'] = dt
        print(f'    >>> Total Time = {time.time() - t0}')
        print(f' >>> {dt} {self.__factor_name} calculation finished')

        return df

    def generate_factor_all(
            self,
            sdt: str,
            edt: str,
            process=1,
            nan_policy='keep'
    ):
        """
        计算因子并录入数据库

        :param process: 线程数，可以多线程加速计算
        :param sdt: (str)起始时间, YYYY-MM-DD
        :param edt: (str)结束时间, YYYY-MM-DD
        :param pickle_path:
        :param if_pickle:
        :return: None
        """

        t0 = time.time()
        print('-' * 10 + f' Begin to fetch data ' + '-' * 10)
        self.prepare_data(sdt, edt)
        print('-' * 10 + f' Fetching finished, time = {round(time.time() - t0)}s ' + '-' * 10)

        self.trading_days = self.get_trading_days(sdt, edt)

        print('-' * 10 + ' Factor Calculation Begin ' + '-' * 10)
        t0 = time.time()

        # 多线程计算
        pool = Pool(process)
        for trading_day in self.trading_days:
            pool.apply_async(
                func=self.get_daily_result,
                args=(trading_day,),
                callback=self.__factor.append,
                error_callback=lambda x: print('Multi-Process Error: ', x)
            )
        pool.close()
        pool.join()

        # 清洗 & 储存
        self.__factor = pd.concat(self.__factor)
        self.__factor.rename(columns={0: self.__factor_name}, inplace=True)
        self.__factor.sort_values('TRADE_DT', ascending=True, inplace=True)
        self.__factor.index = list(range(len(self.__factor)))
        self.clear_factor(nan_policy=nan_policy)
        print('-' * 10 + f' Factor Calculation Done！Time = {round(time.time() - t0)}s ' + '-' * 10)

    def save(
            self,
            if_pickle=False,
            pickle_path=None
    ) -> None:
        """
        储存因子进入数据库
        【注意】：只需要在因子首次计算时调用，后续更新请调用__update_factor()
        """
        if self.__factor is None or len(self.__factor) == 0:
            return

        # 将数据储存在mongo中
        # 储存存储因子的collection名称
        print(f'Begin to save {self.__factor_name} in {self.__save_db}.{self.__factor_name}')
        print(f'Range from {self.__factor["TRADE_DT"].iloc[0]} to {self.__factor["TRADE_DT"].iloc[-1]}')
        collection = client[self.__save_db][self.__factor_name]
        creat_mongodb(self.__factor, collection, 'S_INFO_WINDCODE', 'TRADE_DT')

        # 如果需要储存为pkl
        if if_pickle:
            if pickle_path is None:
                raise NotImplementedError('please enter a pkl saving path!')
            self.__factor.to_pickle(os.path.join(pickle_path, self.__factor_name))

        print('Saving finished!')

    def update_factor(
            self,
            process=1
    ) -> None:
        """
        因子更新自动化函数，调用函数前请确保正确初始化对应因子对象，正确输入因子参数与名称以便client能正确访问到因子数据库
        :return:
        """
        # check if update
        print('Checking for updating...')
        newest_dt_factor = pd.to_datetime(
            client[self.__save_db][self.__factor_name]
                .find().sort([('TRADE_DT', -1)])
                .limit(1)[0]['TRADE_DT']
        ).strftime('%Y-%m-%d')
        newest_dt_price = pd.to_datetime(
            client['basic_data']['Daily_return_with_cap']
                .find().sort([('TRADE_DT', -1)])
                .limit(1)[0]['TRADE_DT']
        ).strftime('%Y-%m-%d')

        if newest_dt_factor >= newest_dt_price:
            print('No need for updating!')
            return

        # if need, cal update range
        sdt = self.TD.offset(newest_dt_factor, 1)
        edt = newest_dt_price
        updating_range = self.get_trading_days(sdt, edt)

        print(
            f'Start updating {self.__factor_name} \n'
            f'Range from {updating_range[0]} to {updating_range[-1]} \n'
            f'Total = {len(updating_range)}days'
        )

        # fetching data
        t0 = time.time()
        print('-' * 10 + f' Begin to fetch data ' + '-' * 10)
        self.prepare_data(sdt, edt)
        print('-' * 10 + f' Fetching finished, time = {round(time.time() - t0)}s ' + '-' * 10)

        self.trading_days = updating_range
        self.__factor = []

        # 多线程计算
        pool = Pool(process)
        for trading_day in self.trading_days:
            pool.apply_async(
                func=self.get_daily_result,
                args=(trading_day,),
                callback=self.__factor.append,
                error_callback=lambda x: print('Multi-Process Error: ', x)
            )
        pool.close()
        pool.join()

        # 储存
        self.__factor = pd.concat(self.__factor)
        self.__factor.rename(columns={0: self.__factor_name}, inplace=True)
        self.__factor.sort_values('TRADE_DT', ascending=True, inplace=True)
        self.__factor.index = list(range(len(self.__factor)))
        self.clear_factor()
        print(f'updating calculation finished, time = {time.time() - t0}s')
        client[self.__save_db][self.__factor_name].insert_many(to_json_from_pandas(self.__factor))
        return

    def test_calculation(self, dt):
        """
        因子调试计算功能，只计算一天的因子进行测试，用于debug，运行前提是已经运行了prepare_data()
        :param dt: 计算因子的日期
        :return:
        """
        return self.get_daily_result(dt)

    def del_factor(self, sdt, edt) -> None:
        """
        删除因子数据库中从sdt到edt的数据
        :param sdt: YYYY-MM-DD
        :param edt: YYYY-MM-DD
        :return:
        """
        edt = self.TD.offset(edt, 1)

        print('-' * 10 + f'Delete {self.__factor_name} data from {self.__save_db}.{self.__factor_name}' + '-' * 10)
        print('-' * 10 + f'Range from {sdt} to {edt}' + '-' * 10)
        client[self.__save_db][self.__factor_name].delete_many(
            {"TRADE_DT": {'$gte': sdt, '$lte': edt}}
        )
        print('-' * 10 + 'Delete Complete' + '-' * 10)

        return
