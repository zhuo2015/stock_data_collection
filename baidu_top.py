import pandas as pd
import json
from pymongo import MongoClient
import requests
import datetime
'''
scrapy baidu top index of securities, include:
000/600, 300, 002,
saving to Mongodb. Baidu_top,
each index as a record or documents in Mongodb
'''
url1 = 'http://top.baidu.com/buzz?b=274&c=17&fr=topcategory_c17'
url2 = 'http://top.baidu.com/buzz?b=277&c=17&fr=topbuzz_b274_c17'
url3 = 'http://top.baidu.com/buzz?b=276&c=17&fr=topbuzz_b277_c17'

url_list = [url1, url2, url3]

client = MongoClient('localhost', 27017)
db = client.stock_db
collection = db.Baidu_top


def get_top_list(url):
    html1 = requests.get(url)
    html1 = html1.content.decode('gb2312')
    top_sec = pd.read_html(html1)[0]
    top_sec_index = top_sec.dropna(axis=0)
    top_sec_index.index = top_sec_index[0]
    top_sec_index['time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    del top_sec_index[0]
    del top_sec_index[2]
    top_sec_index.columns = top_sec_index.iloc[0]
    top_sec_index = top_sec_index.drop('排名')

    return top_sec_index


records = 0

for url_name in url_list:
    df = get_top_list(url_name)
    collection.insert_many(json.loads(df.to_json(orient='records')))
    records += df.shape[0]

print(records)