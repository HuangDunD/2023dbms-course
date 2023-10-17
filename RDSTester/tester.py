from mysql.driver import Driver
from util import *
import time
import random
import math
from record.record import *
from mysql.driver import SQLState
# 输入一个列表，其中每项代表选择该项目的概率，返回选择的项目的下标
def get_choice(choices):
    r = random.random() * sum(choices)
    upto = 0
    for i in range(len(choices)):
        if upto + choices[i] >= r:
            return i
        upto += choices[i]
    assert False, "Shouldn't get here"

def do_test(driver, lock, txns, txn_prob=[0.45, 0.43, 0.04, 0.04, 0.04] ):
    # print(duration)
    # print('Test')
    t_start = time.time()
    for i in range(txns):
        txn = get_choice(txn_prob)
        ret = SQLState.ABORT
        while(ret == SQLState.ABORT):
            if txn == 0: # NewOrder
                d_id = get_d_id()  # 获得地区id，1～10的随机数
                c_id = get_c_id()  # 获得客户id，1～3000的随机数
                ol_i_id = get_ol_i_id()  # 获得新订单中的商品id列表
                ol_supply_w_id = get_ol_supply_w_id(1, driver._scale, len(ol_i_id))  # 为新订单中每个商品选择一个供应仓库，当前设定就一个供应仓库
                ol_quantity = get_ol_quantity(len(ol_i_id))  # 为新订单中每个商品设置购买数量 

                t1 = time.time()
                ret = driver.do_new_order(1, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity)
                t2 = time.time()

                put_new_order(lock, t2-t_start)

            elif txn == 1: # Payment
                d_id = get_d_id()  # 获得地区id，1～10的随机数
                c_w_id, c_d_id = get_c_w_id_d_id(1, d_id, 1)  # 获得客户所属的仓库id和地区id

                t1 = time.time()
                ret = driver.do_payment(1, d_id, c_w_id, c_d_id, query_cus_by(), random.random() * (5000 - 1) + 1)
                t2 = time.time()

            elif txn == 2: # OrderStatus
                t1 = time.time()
                ret = driver.do_order_status(1, get_d_id(), query_cus_by())
                t2 = time.time()

            elif txn == 3: # StockLevel
                t1 = time.time()
                ret = driver.do_stock_level(1, get_d_id(), random.randrange(10, 21))
                t2 = time.time()

            elif txn == 4: # Delivery
                t1 = time.time()
                ret = driver.do_delivery(1, get_o_carrier_id())
                t2 = time.time()

            if ret == SQLState.ABORT:
                put_txn(lock, txn, t2-t1, False)
            else:
                put_txn(lock, txn, t2-t1, True)
            

