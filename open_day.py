from struct import unpack
from pymongo import MongoClient
import pandas as pd
import json
import os

# Open MongoDB
client = MongoClient('localhost', 27017)
db = client.stock_db
collection = db.ZSZQ_Day_stock


def day_stock_k(fileName):
    ofile = open(data_dir+fileName, 'rb')
    buf = ofile.read()
    ofile.close()
    num = len(buf)
    no = num / 32
    b = 0
    e = 32
    items = list()
    code = fileName.strip('.day')
    # code = code.strip('.')
    for i in range(int(no)):
        a = unpack('IIIIIfII', buf[b:e])
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

        openPrice = a[1] / 100.0
        high = a[2] / 100.0
        low = a[3] / 100.0
        close = a[4] / 100.0
        amount = a[5] / 10.0
        vol = a[6]
        unused = a[7]
        if i == 0:
            preClose = close
        ratio = round((close - preClose) / preClose * 100, 2)
        preClose = close
        item = [code, dd, str(openPrice), str(high), str(low), str(close),
                str(ratio), str(amount), str(vol)]
        items.append(item)
        b = b + 32
        e = e + 32

    column = ['code', 'date', 'open', 'high', 'low', 'close', 'ratio', 'amount', 'volume']
    df = pd.DataFrame(items, columns=column)
    return df


dirs = ['sh', 'sz']

for dir_name in dirs:
    data_dir = 'D:\\zszq\\vipdoc\\' + dir_name + '\\lday\\'
    filelist = os.listdir(data_dir)
    for file in filelist:
        day_data = day_stock_k(file)
        collection.insert_many(json.loads(day_data.to_json(orient='records')))