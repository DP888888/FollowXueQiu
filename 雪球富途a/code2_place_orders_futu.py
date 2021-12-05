from futu import *
import datetime
import time
import pandas as pd
import os
import json

pwd_unlock = '256256'
env = TrdEnv.SIMULATE  # 模拟盘
# env = TrdEnv.REAL  # 实盘
meta_holding_constant = 0.2  #总仓位控制
buy_slippage = 1.01
sell_slippage = 0.99
intervals = 0.1  # 下单检测间隔

trd_ctx = []

# 港股开盘收盘时间
open_time = '092900'
close_time = '160100'

def read_data(filename=None):
    if filename == None:
        return None
    if not os.path.exists(filename):
        d = {"update_time": 0}
        return d
    with open(filename, "r", encoding="utf-8") as g:
        ret = json.loads(g.read())
        if ret:
            return ret
        else:
            return None


def get_stock_info(stock_list):
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

    ret, data = quote_ctx.get_market_snapshot(stock_list)
    if ret == RET_OK and data.shape[0] > 0:
        data = data[['code', 'lot_size', 'price_spread']]
        data.columns = ['股票代码', '最小股数', '最小价差']
    else:
        data = pd.DataFrame(columns=['股票代码', '最小股数', '最小价差'])
    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
    return data


def get_total_assets():
    total = 0.0
    ret, data = trd_ctx.accinfo_query(trd_env=env)
    if ret == RET_OK:
        # data.to_csv('test.csv', encoding='gbk')
        total = data['total_assets'][0]
    else:
        print('position_list_query error: ', data)
    return total


def get_holdings():
    ret, data = trd_ctx.position_list_query(trd_env=env)
    if ret == RET_OK and data.shape[0] > 0:
        # data.to_csv('test.csv', encoding='gbk')
        data.loc[data['position_side'] == 'LONG', 'position_side'] = 1.0
        data.loc[data['position_side'] == 'SHORT', 'position_side'] = -1.0
        data['position_side'] = data['position_side'].astype('double')
        data['qty'] = data['qty'] * data['position_side']
        data['can_sell_qty'] = data['can_sell_qty'] * data['position_side']
        data['market_val'] = data['market_val'] * data['position_side']
        data = data[['code', 'qty', 'can_sell_qty', 'cost_price', 'market_val']]
        data.columns = ['股票代码', '持仓', '可用持仓', '成本', '持仓市值']
    else:
        data = pd.DataFrame(columns=['股票代码', '持仓', '可用持仓', '成本', '持仓市值'])
    return data


def get_to_be_traded_orders():
    ret, results = trd_ctx.order_list_query(trd_env=env)
    if ret == RET_OK and results.shape[0] > 0:
        # results.to_csv('test.csv', encoding='gbk')
        results = results[['code', 'qty', 'dealt_qty', 'price', 'trd_side', 'order_status']]
        results.columns = ['股票代码', '委托数量', '成交数量', '委托价格', '委托类型', '委托状态']
        results = results[(results['委托状态'] == 'SUBMITTED') | (results['委托状态'] == 'FILLED_PART')]
        results.loc[results['委托类型'] == 'BUY', '委托类型'] = 1.0
        results.loc[results['委托类型'] == 'SELL', '委托类型'] = -1.0
        results['委托类型'] = results['委托类型'].astype('double')
        results['委托数量'] = results['委托数量'] * results['委托类型']
        results['成交数量'] = results['成交数量'] * results['委托类型']
        results['委托额'] = results['委托数量'] * results['委托价格']
        results = results.groupby(by=['股票代码']).sum().reset_index()
        results['委托价格'] = results['委托额'] / results['委托数量']
        results = results[['股票代码', '委托数量', '成交数量', '委托价格']]
    else:
        results = pd.DataFrame(columns=['股票代码', '委托数量', '成交数量', '委托价格'])
    return results


def trade(code, side, price, qty):
    if side.lower() == 'buy':
        side = TrdSide.BUY
        adj_lmt = 0.01
    elif side.lower() == 'sell':
        side = TrdSide.SELL
        adj_lmt = -0.01
    else:
        print('place_order error: invalid side ' + side)
        return None

    ret, data = trd_ctx.unlock_trade(pwd_unlock)  # 先解锁交易
    if ret == RET_OK:
        ret, data = trd_ctx.place_order(price=price, qty=qty, code=code, trd_side=side, adjust_limit = adj_lmt, trd_env=env)
        if ret == RET_OK:
            return data['order_id'][0]
        else:
            print('place_order error: ', data)
    else:
        print('unlock_trade failed: ', data)
    return None


if __name__ == '__main__':
    pd.set_option('max_columns', None)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 1000)

    print('Futu code2 run ...')
    timestamp = int(round(time.time() * 1000))
    # timestamp = 0  # 调试用
    while True:
        # 如果雪球组合没有更新则无需不断请求API接口
        cmd_data = read_data('record.json')
        if not cmd_data:
            continue
        
        if timestamp < cmd_data['update_time']:
            timestamp = cmd_data['update_time']
        else:
            now_time = datetime.datetime.now().strftime('%H%M%S')
            if ( open_time < now_time < close_time):
                # print('雪球组合未发生变化')
                time.sleep(intervals)
            else:
                # 非开盘时间，每2秒检查一次更新
                time.sleep(5)
                
            continue

        start_time = datetime.datetime.now()

        # 获取雪球组合
        print('雪球组合：')
        xueqiu = pd.read_json(cmd_data['list'])
        xueqiu.columns = ['板块名称', '股票代码', '总资金占比', '目标价格', '股票名称']
        print(xueqiu)
        # 如果是空命令就跳过
        if xueqiu.empty:
            print('空命令，跳过下单')
            continue
        
        ### 交易对象初始化
        trd_ctx = OpenHKTradeContext(host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
        
        # 获取总资产
        total_assets = get_total_assets() * meta_holding_constant
        print('总资产:' + str(total_assets))

        # 获取每手股数
        print('每手股数：')
        stock_info = get_stock_info(xueqiu['股票代码'].to_list())
        print(stock_info)
        xueqiu = xueqiu.merge(stock_info, how='left', left_on='股票代码', right_on='股票代码')
        xueqiu['目标股数'] = xueqiu['总资金占比'] / 100.0 * total_assets / xueqiu['目标价格']
        xueqiu['目标股数'] = (xueqiu['目标股数'] / xueqiu['最小股数']).astype('int64') * xueqiu['最小股数']

        # 获取持仓
        print('持仓:')
        holding = get_holdings()
        print(holding)
        xueqiu = xueqiu.merge(holding, how='left', left_on='股票代码', right_on='股票代码')
        xueqiu = xueqiu.fillna(0.0)

        # 获取委托
        print('汇总:')
        traded_orders = get_to_be_traded_orders()
        xueqiu = xueqiu.merge(traded_orders, how='left', left_on='股票代码', right_on='股票代码')
        xueqiu = xueqiu.fillna(0.0)
        xueqiu['仓位变动'] = xueqiu['目标股数'] - xueqiu['持仓'] - xueqiu['委托数量'] + xueqiu['成交数量']
        xueqiu = xueqiu[xueqiu['仓位变动'] != 0.0]
        print(traded_orders)
        print(xueqiu)
        if not xueqiu.empty:
            filename = './log/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'
            xueqiu.to_csv(filename, encoding='gbk', index=False)
        else:
            print('仓位变动为0，跳过')
            trd_ctx.close()
            continue
            
        # 按照先卖再买的顺序下单
        xueqiu = xueqiu.sort_values(by=['仓位变动'])

        # 下单
        orders = []
        for i in range(xueqiu.shape[0]):
            stock_code = xueqiu.iloc[i, :]['股票代码']
            target_price = xueqiu.iloc[i, :]['目标价格']
            to_be_changed = int(xueqiu.iloc[i, :]['仓位变动'])
            available = int(xueqiu.iloc[i, :]['可用持仓'])
            price_spread = float(xueqiu.iloc[i, :]['最小价差'])

            if to_be_changed > 0:
                p = int(target_price * buy_slippage / price_spread) * price_spread + price_spread
                # p = target_price * buy_slippage
                print('买入：', stock_code, int(to_be_changed), p)
                order_id = trade(stock_code, 'buy', p, int(to_be_changed))
                if order_id:
                    orders.append(order_id)
            elif to_be_changed < 0:
                if available < int(abs(to_be_changed)):
                    to_be_changed = -available

                if to_be_changed != 0:
                    p = int(target_price * sell_slippage / price_spread) * price_spread
                    # p = target_price * sell_slippage
                    print('卖出：', stock_code, int(abs(to_be_changed)), p)
                    order_id = trade(stock_code, 'sell', p, int(abs(to_be_changed)))
                    if order_id:
                        orders.append(order_id)
            else:
                continue

        ### 完成下单，关闭链接
        trd_ctx.close()

        print('--------总用时---------', (datetime.datetime.now() - start_time).microseconds / 1e6)  # us
