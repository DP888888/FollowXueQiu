import datetime
import random
from multiprocessing.dummy import Pool as ThreadPool

import pandas as pd
import requests
import time
import os
import json
import re

pool_num = 3  # 线程数
port_list = list(range(24000, 24401))  # 端口列表
filename = "record.json"    # 记录文件名称（默认同级目录下）
intervals = 0.5   # 间隔时间 单位：秒
# live_time = 18000   # 2*60, 设置默认爬虫生存时间2分钟

# 港股开盘收盘时间
open_time  = '092900'
close_time = '160100'

# 雪球组合
portfolio_code = "ZH1218504"    # 港股-实盘刷妖，liaode
portfolio_name = "(港股-实盘刷妖, liaode)"


headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
              "application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "xueqiu.com",
    "Pragma": "no-cache",
    "sec-ch-ua-mobile": "?0",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/89.0.4389.114 Safari/537.36",
}


def unix2date(unix_time, format_type="%Y-%m-%d %H:%M:%S"):
    # 时间戳转化日期
    return time.strftime(format_type, time.localtime(int(unix_time/1000)))


def gen_proxies():
    # 建立代理
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": "haproxy.virjar.com",
        "port": random.randint(24000, 24400),
        "user": "admin8",
        "pass": "szzs000001",
    }
    proxies = {
        "http": proxyMeta,
        "https": proxyMeta,
    }
    return proxies


def read_data():
    """
    :return: 上次记录的更新时间
    """
    if not os.path.exists(filename):
        d = {"update_time": 0}
        return d
    with open(filename, "r", encoding="utf-8") as g:
        ret = json.loads(g.read())
        if ret:
            return ret
        else:
            return None


def write_data(data):
    """
    写入最新更新时间
    """
    with open(filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False))


def spider(port):
    # 计算线程生存时间
    # time_to_end = int(round(time.time())) + live_time
    # print('new spider start')
    while True:
        # if int(round(time.time())) > time_to_end:
        #    print('this spider() end')
        #    return
        # 港股上午收盘后到下午开盘前，休眠
        if '125900' > (datetime.datetime.now().strftime('%H%M%S')) > '120000':
            time.sleep(5)
            return

        # 到港股收盘时间程序退出
        if (datetime.datetime.now().strftime('%H%M%S')) > close_time:
            print('close time')
            return
        time.sleep(intervals)
        
        # 获取数据
        try:
            r = requests.get(url='https://xueqiu.com/P/' + portfolio_code, headers=headers, timeout=2, proxies=gen_proxies())
            print(portfolio_code + ' ' + (datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')), end='\r')
            result = json.loads(re.findall(r"cubeInfo[ ]?=[ ]?(.*?);", r.text)[0])
            holding_data = result["view_rebalancing"]
            trade_data = result["sell_rebalancing"]
            record_data = read_data()
            if not record_data:
                print("文件读取返回None")
                continue
            latest_update_time = int(result["view_rebalancing"]["updated_at"])
            if latest_update_time > record_data["update_time"]:
                print()
                print("最新调仓：{}".format(unix2date(latest_update_time)))
                holding_df = pd.DataFrame(holding_data["holdings"])
                holding_df = holding_df[['segment_name', 'stock_symbol', 'weight']]
                trade_df = pd.DataFrame(trade_data["rebalancing_histories"])
                trade_df = trade_df[['stock_symbol', 'price', 'stock_name']]
                holding_df = holding_df.merge(trade_df, how='right', left_on='stock_symbol', right_on='stock_symbol')
                holding_df = holding_df[holding_df['price'] > 0.0]
                if holding_df.empty:
                    print('调仓价格为0，不更新时间戳')
                    latest_update_time = record_data["update_time"]
                holding_df['stock_symbol'] = 'HK.' + holding_df['stock_symbol']
                holding_df['segment_name'] = holding_df['segment_name'].fillna('')
                holding_df['weight'] = holding_df['weight'].fillna(0.0)
                print(holding_df)
                obj = {"update_time": latest_update_time, "list": holding_df.to_json(force_ascii=False)}
                record_data.update(obj)
                write_data(record_data)
                csv_filename = './log/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S_') + portfolio_code + '_' + str(latest_update_time) + '.csv'
                holding_df.to_csv(csv_filename, encoding='gbk', index=False)
        except Exception as e:
            # print(e)
            pass

def run():
    # 根据时间建立多线程，获取数据任务
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 1000)
    print('跟踪组合: ' + portfolio_code + portfolio_name)
    while True:
        if close_time > (datetime.datetime.now().strftime('%H%M%S')) > open_time:
            pool = ThreadPool(pool_num)
            pool.map(spider, port_list[-pool_num:])
            pool.close()
            pool.join()
        else:
            #开盘外时间，休眠！
            time.sleep(5)
    

if __name__ == '__main__':
    pd.set_option('max_columns', None)
    run()

