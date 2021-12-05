import random
import time
import pandas as pd
import os
import json
import numpy as np

def read_data(filename):
    if not os.path.exists(filename):
        d = {"update_time": 0}
        return d
    with open(filename, "r", encoding="utf-8") as g:
        return json.loads(g.read())

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', 1000)
pd.set_option('max_columns', None)

# 获取雪球组合
record_data = read_data('record.json')
cmd_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(record_data["update_time"]/1000)))
xueqiu = pd.read_json(record_data['list'])
print('雪球组合：', cmd_time)
print(xueqiu)
input('回车退出')