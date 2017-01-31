from struct import *
import json
import pandas as pd
import os
from pymongo import MongoClient
import re


# Open MongoDB
client = MongoClient('localhost', 27017)
db = client.stock_db
collection = db.ZSZQ_M1_others


def m1_to_dataframe(filename):
    ofile = open(data_dir+filename, 'rb')
    buf = ofile.read()
    ofile.close()
    num = len(buf)
    no = num // 32
    b = 0
    e = 32
    dl = []
    code = re.findall(r'\d+#(.*)\.lc1', filename)[0]
    for i in range(no):
        a = unpack('hhffffiii', buf[b:e])

        dl.append(
            [code, str(int(a[0] / 2048) + 2004) + '-' + str(int(a[0] % 2048 / 100)).zfill(2) + '-' +
             str(((a[0]%2048) % 100)).zfill(2),
             str(int(a[1] / 60)).zfill(2) + ':' + str(a[1] % 60).zfill(2) + ':00', a[2], a[3], a[4], a[5], a[6], a[7]])

        b += 32
        e += 32
    df = pd.DataFrame(dl, columns=['code', 'date', 'time', 'open', 'high', 'low', 'close', 'amount', 'volume'])
    return df


# 遍历指定的文件夹
records = 0
data_dir = 'D:\\zszq\\vipdoc\\ds\\minline\\'
filelist = os.listdir(data_dir)

for file in filelist:
    m1_data = m1_to_dataframe(file)
    collection.insert_many(json.loads(m1_data.to_json(orient='records')))
    records += m1_data.shape[0]

print(records)

