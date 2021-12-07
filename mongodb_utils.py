import pandas as pd
import pymongo
import json
from tqdm import tqdm


def to_json_from_pandas(data):
    """
    explanation:
        将pandas数据转换成json格式
    params:
        * data ->:
            meaning: pandas数据
            type: null
            optional: [null]
    return:
        dict
    demonstrate:
        Not described
    output:
        Not described
    """

    """需要对于TRADE_DT进行转换, 以免直接被变成了时间戳"""
    if 'TRADE_DT' in data.columns:
        data.TRADE_DT = data.TRADE_DT.apply(str)
    return json.loads(data.to_json(orient='records'))


def creat_mongodb(data, collection, id_index, time_index):
    """
    在mongodb数据库中创建新的collection，把data存入该collection中，并制定索引
    """
    collection.create_index(
        [
            (id_index,
             pymongo.ASCENDING),
            (time_index,
             pymongo.ASCENDING)
        ]
    )

    period = 100000
    l = data.shape[0]
    for i in tqdm(range(0, l, period)):
        start_index = i
        end_index = i + period if i + period <= l else l
        print('Start Transforming')
        temp = to_json_from_pandas(data.iloc[start_index:end_index, :])
        print('Transform successfully')
        print('Start Insert mongodb')
        collection.insert_many(temp)
        print('Insert successfully')
    print('End mongodb')
    return 0


def insert_new_data(data, collection, time_query_key='TRADE_DT', date_list=None):
    """
    插入每天新的数据，会自动检索数据库中最新的日期，只插入最新日期至今的数据
    """

    if ~data.empty:
        data = data.drop_duplicates()
        newest_date_inDB = pd.to_datetime(
            pd.DataFrame.from_records(
                list(
                    collection.find().sort([(time_query_key, -1)]).limit(1)
                )
            )[time_query_key][0][:10]
        )

        if date_list is None:
            Data_insert = data[data[time_query_key] >= newest_date_inDB]
        else:
            Data_insert = data[data[time_query_key].isin(date_list)]
        # 删除数据库中最新一天的数据，存入那天开始至今的数据
        delete_condition = {time_query_key: newest_date_inDB}
        collection.delete_many(delete_condition)
        # 如果dataframe不大，直接存数据库
        # 如果dataframe过大，则一部分一部分存
        if Data_insert.shape[0] <= 100000:
            print()
            print('Start Transforming')
            temp = to_json_from_pandas(Data_insert)
            print('Transform successfully')
            print('Start Insert mongodb')
            collection.insert_many(temp)
            print('End mongodb')
        else:  # 一次存100000条
            period = 100000
            l = Data_insert.shape[0]
            for i in tqdm(range(0, l, period)):
                print(str(i / l * 100) + '%')
                start_index = i
                end_index = i + period if i + period <= l else l
                print()
                print('Start Transforming')
                temp = to_json_from_pandas(Data_insert.iloc[start_index:end_index, :])
                print('Transform successfully')
                print('Start Insert mongodb')
                collection.insert_many(temp)
                print('Insert successfully')
            print('End mongodb')
    else:
        print('The data is empty! Please Check your input!')
        return 0


def insert_new_factor(data, collection, factor_name, time_query_key='TRADE_DT'):
    """
    向数据库中插入新一列因子值，不能包含其他因子，格式一致
    输入factor_name 为 string 类型，需要与data中的列名一致
    """

    if ~data.empty:
        newest_date_inDB = pd.to_datetime(
            pd.DataFrame.from_records(list(collection.find().sort([(time_query_key, -1)]).limit(1)))[time_query_key][0][
            :10])
        Data_insert = data[data[time_query_key] <= newest_date_inDB]

        # 如果dataframe不大，直接存数据库
        # 如果dataframe过大，则一部分一部分存
        if Data_insert.shape[0] <= 100000:
            print()
            print('Start Transforming')
            temp = to_json_from_pandas(Data_insert)
            print('Transform successfully')
            print('Start Insert mongodb')
            for i in tqdm(temp):
                collection.update_one({'S_INFO_WINDCODE': i['S_INFO_WINDCODE'], time_query_key: i[time_query_key]},
                                      {'$set': {factor_name: i[factor_name]}})

            print('End mongodb')
        else:  # 一次存100000条
            period = 100000
            l = Data_insert.shape[0]
            for i in range(0, l, period):
                print(str(i / l * 100) + '%')
                start_index = i
                end_index = i + period if i + period <= l else l
                print()
                print('Start Transforming')
                temp = to_json_from_pandas(Data_insert.iloc[start_index:end_index, :])
                print('Transform successfully')
                print('Start Insert mongodb')
                for i in tqdm(temp):
                    collection.update_one({'S_INFO_WINDCODE': i['S_INFO_WINDCODE'], time_query_key: i[time_query_key]},
                                          {'$set': {factor_name: i[factor_name]}})
                print('Insert successfully')
            print('End mongodb')
    else:
        print('The data is empty! Please Check your input!')
        return 0


def fetch_data(start_date, end_date, collection, time_query_key='TRADE_DT', factor_ls=None):
    """
    从数据库中读取需要指定日期范围的数据,包含startdate，包含enddate

    start_date:which date your need factors, str,'1991-01-01'
    end_date:str,'1991-01-01'
    collection: select collection after connecting to mongodb, such as: 'TRADE_DT'
    time_query_key: str, time key name for query database
    save_list: list with variable your need, make sure your variable is right,
                default= 'all',get all data

    比如，当我需要从Mongodb数据库中factor数据中获取factor这个collection，需要按照以下命令：
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.factor
    collection = db.factor

    注意 TODO：目前function不能一次取超过3年的数据，否则内存要爆，要取全部年份，需要写循环
    """
    if end_date is not None:
        # 将end-date延后一天，以便形成闭区间
        end_date = (pd.to_datetime(end_date) + pd.Timedelta(1, unit='d')).strftime('%Y-%m-%d')
        query = {time_query_key: {"$gte": start_date, "$lte": end_date}}
    else:
        query = {time_query_key: {'$gte': start_date}}

    print('Querying......')

    if factor_ls is not None:
        fields = dict.fromkeys(factor_ls, 1)
        cursor = collection.find(query, fields)
    else:
        cursor = collection.find(query)
    data = pd.DataFrame.from_records(cursor)

    if len(data) != 0:
        data[time_query_key] = pd.to_datetime(data[time_query_key])
    return data
