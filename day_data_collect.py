import tushare as ts
import re
import json
from pymongo import MongoClient

# 指定日期范围
day_1st = '2014-01-01'
# day_end = '2016-12-31'
k_type = '5'

counter = 0  # 统计获取到数据的股票数量
error_code = []  # 记录未取到数据的股票代码

# 打开MongoDB
client = MongoClient('localhost', 27017)
db = client.stock_db
collection = db.stock_D_all_k

# 获取股票代码列表
df_stock_list = ts.get_stock_basics()
df_stock_list_name = df_stock_list['name']
code_dict = {'60': [], '30': [], '00': []}

for code in df_stock_list_name.index:
    b3 = re.findall(r'^(\d{2})\d*', code)[0]
    code_dict[b3].append(code)

code_dict['60'].sort()
code_dict['30'].sort()
code_dict['00'].sort()

for i in code_dict:
    for code in code_dict[i]:
        # 获取某只股票在指定日期范围内的日K线级别数据
        try:
            day_data = ts.get_hist_data(code)
            day_data['code'] = code
            day_data['date'] = day_data.index
            # saving to mongodb
            collection.insert_many(json.loads(day_data.to_json(orient='records')))
            counter += 1
        except (AttributeError, TypeError):
            error_code.append(code)
            print(code)
            continue

# collection.inser_one(json.load(error_code.to_json))
print(counter, '\n', len(error_code), '\n', error_code)