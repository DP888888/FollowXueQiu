import datetime
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant
from xtquant import xtdata
import random
import time
import pandas as pd
import os
import json
import numpy as np

meta_holding_constant = 0.25 #总仓位控制
buy_slippage = 1.01
sell_slippage = 0.99
intervals = 0.1 # 下单检测间隔，单位：秒


# 模拟盘账号
# ht_acct = '18800283' # gubo
# ht_acct = '18800300' # chenyk
# ht_path = r'D:\e海方舟-量化交易版模拟\userdata_mini'

# 实盘账号
ht_acct = '2630116276'
ht_path = r'D:\e海方舟-量化交易版\userdata_mini'


class MyXtQuantTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        """
        连接断开
        :return:
        """
        print("connection lost")

    def on_stock_order(self, order):
        """
        委托回报推送
        :param order: XtOrder对象
        :return:
        """
        #print("on order callback:")
        print("on order callback", order.stock_code, order.order_status, order.order_sysid)

    def on_stock_asset(self, asset):
        """
        资金变动推送
        :param asset: XtAsset对象
        :return:
        """
        #print("on asset callback")
        print("on asset callback", asset.account_id, asset.cash, asset.total_asset)

    def on_stock_trade(self, trade):
        """
        成交变动推送
        :param trade: XtTrade对象
        :return:
        """
        #print("on trade callback")
        print("on trade callback", trade.account_id, trade.stock_code, trade.order_id)

    def on_stock_position(self, position):
        """
        持仓变动推送
        :param position: XtPosition对象
        :return:
        """
        #print("on position callback")
        print("on position callback", position.stock_code, position.volume)

    def on_order_error(self, order_error):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """
        #print("on order_error callback")
        print("on order_error callback", order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        #print("on cancel_error callback")
        print("on cancel_error callback",cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response):
        """
        异步下单回报推送
        :param response: XtOrderResponse 对象
        :return:
        """
        #print("on_order_stock_async_response")
        print("on_order_stock_async_response", response.account_id, response.order_id, response.seq)


def read_data(filename):
    if not os.path.exists(filename):
        d = {"update_time": 0}
        return d
    with open(filename, "r", encoding="utf-8") as g:
        return json.loads(g.read())


def get_total(acc):
    # 获取总资产
    asset = xt_trader.query_stock_asset(acc)
    return asset.total_asset


def get_holding(acc):
    # 获取持仓
    temp = []
    for p in xt_trader.query_stock_positions(acc):
        temp.append([p.stock_code, p.volume, p.can_use_volume, p.open_price, p.market_value])
    temp = pd.DataFrame(temp)
    if temp.empty:
        temp = pd.DataFrame(columns=['股票代码', '持仓', '可用持仓', '成本', '持仓市值'])
    else:
        temp.columns = ['股票代码', '持仓', '可用持仓', '成本', '持仓市值']
    temp['可用持仓'] = temp['可用持仓'].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return temp


def get_to_be_traded_orders(acc):
    # 获取委托
    to_be_traded = xt_trader.query_stock_orders(acc)
    temp = []
    for i in range(len(to_be_traded)):
        # print("{0} {1} {2} {3} {4} {5} {6} {7}".format(to_be_traded[i].order_id, to_be_traded[i].order_time, to_be_traded[i].stock_code, to_be_traded[i].order_volume, to_be_traded[i].traded_volume, to_be_traded[i].price, to_be_traded[i].order_type, to_be_traded[i].order_status))
        temp.append([to_be_traded[i].stock_code, to_be_traded[i].order_volume, to_be_traded[i].traded_volume, to_be_traded[i].price, to_be_traded[i].order_type, to_be_traded[i].order_status])
    column_names = ['股票代码', '委托数量', '成交数量', '委托价格', '委托类型', '委托状态']
    to_be_traded = pd.DataFrame(temp, columns=column_names)
    if to_be_traded.empty:
        return to_be_traded
    to_be_traded = to_be_traded[(to_be_traded['委托状态'] == xtconstant.ORDER_REPORTED) | (to_be_traded['委托状态'] == xtconstant.ORDER_PART_SUCC)]
    to_be_traded.loc[to_be_traded['委托类型'] == 23, '委托类型'] = 1
    to_be_traded.loc[to_be_traded['委托类型'] == 24, '委托类型'] = -1
    to_be_traded['委托数量'] = to_be_traded['委托数量'] * to_be_traded['委托类型']
    to_be_traded['成交数量'] = to_be_traded['成交数量'] * to_be_traded['委托类型']
    to_be_traded['委托额'] = to_be_traded['委托数量'] * to_be_traded['委托价格']
    to_be_traded = to_be_traded.groupby(by=['股票代码']).sum().reset_index()
    to_be_traded['委托价格'] = to_be_traded['委托额'] / to_be_traded['委托数量']
    to_be_traded = to_be_traded[['股票代码', '委托数量', '成交数量', '委托价格']]
    return to_be_traded


def valid_price(price, down_stop_price, up_stop_price):
    if price > up_stop_price:
        return up_stop_price
    if price < down_stop_price:
        return down_stop_price
    return np.round(price, 2)


if __name__ == '__main__':
    # path为mini qmt客户端安装目录下userdata_mini路径
    pd.set_option('max_columns', None)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 1000)

    # session_id为会话编号，策略使用方对于不同的Python策略需要使用不同的会话编号
    session_id = random.randint(10224, 92024)
    xt_trader = XtQuantTrader(ht_path, session_id)
    # 创建普通账号为1882630888的证券账号对象
    acc = StockAccount(ht_acct, account_type='STOCK')

    # 创建交易回调类对象，并声明接收回调
    callback = MyXtQuantTraderCallback()
    xt_trader.register_callback(callback)
    # 启动交易线程
    xt_trader.start()
    # 建立交易连接，返回0表示连接成功
    connect_result = xt_trader.connect()
    print("交易连接成功！") if connect_result == 0 else print("交易连接失败！")

    # 对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功
    subscribe_result = xt_trader.subscribe(acc)
    print("回调进行订阅成功！") if subscribe_result == 0 else print("回调进行订阅失败！")

    # 程序启动把当前持仓显示出来
    print("持仓数量：")
    holding = get_holding(acc)
    print(holding)
    print('等待下单json更新。。。')

    #total = get_total(acc)
    #print(total)

    timestamp = int(round(time.time() * 1000))
    # timestamp = 0 #调试用
    while True:
        # 如果雪球组合没有更新则无需不断请求API接口
        cmd_data = read_data('record.json')
        if timestamp < cmd_data['update_time']:
            timestamp = cmd_data['update_time']
        else:
            now_time = datetime.datetime.now().strftime('%H%M%S')
            if ('092400' < now_time < '153000'):
                # print('雪球组合未发生变化')
                time.sleep(intervals)
            else:
                # 非开盘时间，每2秒检查一次更新
                time.sleep(2)
                
            continue

        start_time = datetime.datetime.now()
        # 获取总资产
        print("总资产：", )
        total = get_total(acc)
        print(total)
        total = total * meta_holding_constant

        # 获取雪球组合
        print('雪球组合：')
        xueqiu = pd.read_json(cmd_data['list'])
        print(xueqiu)
        if not xueqiu.empty:
            xueqiu = xueqiu[~xueqiu['stock_symbol'].str.startswith('688')]
            # xueqiu = xueqiu[~xueqiu['stock_name'].str.contains('ST')]
        xueqiu.columns = ['板块名称', '股票代码', '总资金占比', '目标价格', '股票名称']
        if xueqiu.empty:
            print('下单命令为空，跳过')
            continue

        # 获取持仓
        positions = xt_trader.query_stock_positions(acc)
        print("持仓数量：", len(positions))
        temp = []
        for p in positions:
            temp.append([p.stock_code, p.volume, p.can_use_volume, p.open_price, p.market_value])
        temp = pd.DataFrame(temp)
        if temp.empty:
            temp = pd.DataFrame(columns=['股票代码', '持仓', '可用持仓', '成本', '持仓市值'])
        temp.columns = ['股票代码', '持仓', '可用持仓', '成本', '持仓市值']
        temp['可用持仓'] = temp['可用持仓'].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        holding = temp
        print(holding)
        xueqiu = xueqiu.merge(holding, how='left', left_on='股票代码', right_on='股票代码')

        xueqiu['最小股数'] = 100.0
        # xueqiu.loc[xueqiu['板块名称'] == '可转债', '最小股数'] = 10.0
        xueqiu.loc[xueqiu['股票代码'].str.startswith('1'), '最小股数'] = 10.0  # 可转债1手10股
        xueqiu = xueqiu.fillna(0.0)
        xueqiu['目标股数'] = xueqiu['总资金占比'] / 100.0 * total / xueqiu['目标价格']
        xueqiu['目标股数'] = (xueqiu['目标股数'] / xueqiu['最小股数']).astype('int64') * xueqiu['最小股数']

        # 获取汇总表格，加上委托数据
        print('汇总：')
        xueqiu = xueqiu.merge(get_to_be_traded_orders(acc), how='left', left_on='股票代码', right_on='股票代码')
        xueqiu = xueqiu.fillna(0.0)
        xueqiu['仓位变动'] = xueqiu['目标股数'] - xueqiu['持仓'] - xueqiu['委托数量'] + xueqiu['成交数量']
        xueqiu = xueqiu[xueqiu['仓位变动'] != 0.0]
        print(xueqiu)
        if not xueqiu.empty:
            filename = './log/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'
            xueqiu.to_csv(filename, encoding='gbk', index=False)
        else:
            print('仓位变动为0，跳过')
            continue

        # 按照先卖再买的顺序下单
        xueqiu = xueqiu.sort_values(by=['仓位变动'])

        orders = []
        for i in range(xueqiu.shape[0]):
            stock_code = xueqiu.iloc[i, :]['股票代码']
            target_price = xueqiu.iloc[i, :]['目标价格']
            to_be_changed = int(xueqiu.iloc[i, :]['仓位变动'])
            available = int(xueqiu.iloc[i, :]['可用持仓'])

            if to_be_changed > 0:
                instrument_detail = xtdata.get_instrument_detail(stock_code)
                # print(stock_code, instrument_detail)
                p = valid_price(target_price * buy_slippage, instrument_detail['DownStopPrice'], instrument_detail['UpStopPrice'])
                print('买入：', stock_code, int(abs(to_be_changed)), p)
                order_id = xt_trader.order_stock(acc, stock_code, xtconstant.STOCK_BUY, int(abs(to_be_changed)),
                                                 xtconstant.FIX_PRICE, p, 'strategy_name', 'remark')
                orders.append(order_id)
            elif to_be_changed < 0:
                if available < int(abs(to_be_changed)):
                    to_be_changed = -available

                if to_be_changed != 0:
                    instrument_detail = xtdata.get_instrument_detail(stock_code)
                    p = valid_price(target_price * sell_slippage, instrument_detail['DownStopPrice'], instrument_detail['UpStopPrice'])
                    print('卖出：', stock_code, int(abs(to_be_changed)), p)
                    order_id = xt_trader.order_stock(acc, stock_code, xtconstant.STOCK_SELL, int(abs(to_be_changed)),
                                                     xtconstant.FIX_PRICE, p, 'strategy_name', 'remark')
                    orders.append(order_id)
            else:
                continue

        print('--------总用时---------', (datetime.datetime.now() - start_time).microseconds / 1e6)  # us
