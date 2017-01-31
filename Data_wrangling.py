from pymongo import MongoClient
import pandas as pd
import json

query = {'code': 'RBL8'}
# query = {'code': 'IL8'}

client = MongoClient('localhost', 27017)
db = client.stock_db
collection = db.ZSZQ_M5_others


def data_1st_wrangling(query1):
    '''
    data wrangling for data from mongodb
    query = {'code': 'RBL8'}
    '''

    cursor = collection.find(query1)

    df = pd.DataFrame(list(cursor))

    # drop duplicates rows
    del df['_id']
    df = df.drop_duplicates()

    # reindex by column 'date+time'
    df['index1'] = df.date + ' ' + df.time
    df.date = pd.to_datetime(df.index1)
    df.index = df.index1
    df = df.sort_index(ascending=True)

    # drop useless columns and reindex by columns
    # del df['index1']
    del df['date']
    del df['time']
    df = df[['index1', 'open', 'high', 'low', 'close', 'position', 'amount', 'code']]

    return df


# Algorithm for 1st step
# f(x) = y

if __name__ == '__main__':
    df1 = data_1st_wrangling(query)
    db_name = 'db.' + 'ZSZQ_' + query['code']
    coll1 = db.ZSZQ_RBL8
    coll1.insert_many(json.loads(df1.to_json(orient='records')))
    print(df1.head())
    print(df1.tail())
