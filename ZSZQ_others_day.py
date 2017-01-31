from struct import unpack
from pymongo import MongoClient
import pandas as pd
import re
import json
import os

# Open MongoDB
client = MongoClient('localhost', 27017)
db = client.stock_db
collection = db.ZSZQ_Day_others


def futures_day_data(fileName):

    code = re.findall(r'\d+#(.*)\.day', fileName)[0]

    ofile = open(data_dir+fileName, 'rb')
    buf = ofile.read()
    ofile.close()
    num = len(buf)
    no = num / 32
    b = 0
    e = 32
    items = list()
    for i in range(int(no)):
        # a = unpack('IIIIIfII', buf[b:e])
        a = unpack('IffffIIf', buf[b:e])

        year = int(a[0] / 10000);
        m = int((a[0] % 10000) / 100);
        month = str(m);
        if m < 10:
            month = "0" + month;
        d = (a[0] % 10000) % 100;
        day = str(d);
        if d < 10:
            day = "0" + str(d);
        dd = str(year) + "-" + month + "-" + day
        openPrice = a[1]
        high = a[2]
        low = a[3]
        close = a[4]
        positions = a[5]
        amount = a[6]
        settlement = a[7]
        if i == 0:
            preClose = close
        ratio = round((close - preClose) / preClose * 100, 2)
        preClose = close
        item = [code, dd, openPrice, high, low, close, ratio, positions, amount, settlement]
        items.append(item)
        b += 32
        e += 32

    column = ['code', 'date', 'open', 'high', 'low', 'close', 'ratio', 'positions', 'volume', 'settlement']
    df = pd.DataFrame(items, columns=column)

    return df


data_dir = 'D:\\zszq\\vipdoc\\ds\\lday\\'
filelist = os.listdir(data_dir)
records = 0

for file in filelist:
    day_data = futures_day_data(file)
    collection.insert_many(json.loads(day_data.to_json(orient='records')))
    records += day_data.shape[0]

print(records)
