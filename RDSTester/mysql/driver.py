import re
import time
from db.rmdb_client import Client
from decimal import Decimal
from queue import Queue
from threading import Thread
from db.table_layouts import *
from mysql.sql import *
from util import *
from record.record import *

class Driver:
    def __init__(self, scale):
        self._scale = scale
        self._client = Client()
        self._flag = True
        # self._delivery_q = Queue()
        # self._delivery_t = Thread(target=self.process_delivery, args=(self._delivery_q,))
        # self._delivery_t.start()
        # self._delivery_stop = False

    def delay_close(self):
        self._flag = False
        # while not self._delivery_stop:
        #     continue
        self._client.close()
    
    # def close(self):
    #     self._client.close()


    def build(self):
        print("Build table schema...")
        sql = open("RDSTester/db/create_tables.sql", "r").read().split(';')
        for line in sql:
            if (line):
                self._client.send_cmd(line + ';')

    def load(self):
        print("Load table data...")
        sql = open("RDSTester/db/load_csvs.sql", "r").read().split(';')
        for line in sql:
            if (line):
                self._client.send_cmd(line + ';')
        print('Database has been initialized.')             
        
    def create_index(self):
        print("Create index...")
        sql = open("RDSTester/db/create_index.sql", "r").read().split(';')
        for line in sql:
            if (line):
                self._client.send_cmd(line + ';')

    def do_new_order(self, w_id, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity):
        ol_cnt = len(ol_i_id)
        ol_amount = 0
        total_amount = 0
        brand_generic = ''

        #transcation
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ New Order')
        #phase 1
        try:
            res = select(client=self._client,
                                        table=DISTRICT,
                                        col = (D_TAX,D_NEXT_O_ID),
                                        where=[(D_ID,eq,d_id),
                                            (D_W_ID,eq,w_id)])
            
            # 每一个都加上判断
            if res == SQLState.ABORT:
                return SQLState.ABORT
            
            d_tax, d_next_o_id = res[0]
            d_tax = eval(d_tax); d_next_o_id = eval(d_next_o_id)
        except Exception as e:
            d_tax = 0; d_next_o_id = 0

        if update(client=self._client,
               table=DISTRICT,
               row=(D_NEXT_O_ID,d_next_o_id+1),
               where=[(D_ID,eq,d_id),(D_W_ID,eq,w_id)]) == SQLState.ABORT:
            return SQLState.ABORT
        
        try:
            res = select(client=self._client,
                        col=[C_DISCOUNT, C_LAST, C_CREDIT, W_TAX],
                        table=[CUSTOMER, WAREHOUSE],
                        where=[(W_ID,eq,w_id),(C_W_ID,eq,W_ID),(C_D_ID,eq,d_id),(C_ID,eq,c_id)]
                            )
            if res == SQLState.ABORT:
                return SQLState.ABORT
            c_discount, c_last_, c_credit, w_tax = res[0]
            c_discount = eval(c_discount)
            w_tax = eval(w_tax)
        except Exception as e:
            c_discount = 0
            w_tax = 0

        #phase 2
        order_time = "'"+current_time()+"'"
        if insert(client=self._client,
               table=ORDERS, 
               rows=(d_next_o_id,d_id,w_id,c_id,order_time,0,ol_cnt,int(len(set(ol_supply_w_id))==1))) == SQLState.ABORT:
            return SQLState.ABORT
        
        if insert(client=self._client,
                table=NEW_ORDERS,
               rows=(d_next_o_id,d_id,w_id)) == SQLState.ABORT:
            return SQLState.ABORT
        #phase 3
        for i in range(ol_cnt):
            try: 
                res = select(client=self._client,
                            table=ITEM,
                            col=(I_PRICE,I_NAME,I_DATA),
                            where=(I_ID,eq,ol_i_id[i]))
                if res == SQLState.ABORT:
                    return SQLState.ABORT
                i_price, i_name, i_data = res[0]
                i_price = eval(i_price)
            except Exception as e:
                i_price = 1; i_data='null'

            try:
                res = select(client=self._client,
                    table=STOCK,
                    col=(S_QUANTITY,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,
                            S_DIST_08,S_DIST_09,S_DIST_10,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA),
                    where=[(S_I_ID,eq,ol_i_id[i]),
                            (S_W_ID,eq,ol_supply_w_id[i])])
                if res == SQLState.ABORT:
                    return SQLState.ABORT
                _quantity, *s_dist, s_ytd, s_order_cnt, s_remote_cnt, s_data= res[0]
                s_quantity = eval(s_quantity); s_ytd = eval(s_ytd); s_order_cnt = eval(s_order_cnt); s_remote_cnt = eval(s_remote_cnt); 
            except Exception as e:
                s_quantity = 0; s_ytd = 0; s_order_cnt = 0; s_remote_cnt = 0; s_dist = []
            
            if s_quantity - ol_quantity[i] >= 10:
                s_quantity -= ol_quantity[i]
            else:
                s_quantity = s_quantity - ol_quantity[i] + 91
            s_ytd += ol_quantity[i]
            s_order_cnt += 1
            if ol_supply_w_id[i] != w_id:
                s_remote_cnt += 1
            if update(client=self._client,
                   table=STOCK,
                   row=[(S_QUANTITY,s_quantity),
                        (S_YTD,s_ytd),
                        (S_ORDER_CNT,s_order_cnt),
                        (S_REMOTE_CNT,s_remote_cnt)],
                   where=[(S_I_ID, eq, ol_i_id[i]),
                          (S_W_ID, eq, ol_supply_w_id[i])]) == SQLState.ABORT:
                          return SQLState.ABORT
            ol_amount = ol_quantity[i] * i_price
            brand_generic = 'B' if re.search('ORIGINAL',i_data) and re.search('ORIGINAL',s_data) else 'G'
            try:
                if insert(client=self._client,
                    table=ORDER_LINE,
                    rows=(d_next_o_id,d_id,w_id,i,ol_i_id[i],ol_supply_w_id[i],order_time,ol_quantity[i],ol_amount,"'"+s_dist[d_id-1]+"'")) == SQLState.ABORT:
                    return SQLState.ABORT
                
            except Exception as e:
                pass
            total_amount += ol_amount
        total_amount *=(1-c_discount)*(1+w_tax+d_tax)
        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- New Order')
        return SQLState.SUCCESS


    def do_payment(self, w_id, d_id, c_w_id, c_d_id, c_query, h_amount):
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ Payment')
        try:
            res = select(client=self._client,
                table=WAREHOUSE,
                col=(W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_YTD),
                where=(W_ID,eq,w_id))
            if res == SQLState.ABORT:
                return SQLState.ABORT
            w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd = res[0]
        except Exception as e:
            w_name, d_name = 'null', 'null'
        # w_ytd = eval(w_ytd)
        if update(client=self._client,
               table=WAREHOUSE,
               row=(W_YTD,W_YTD+'+'+str(h_amount)),
               where=(W_ID,eq,w_id)) == SQLState.ABORT:
               return SQLState.ABORT
        try:
            res = select(client=self._client,
                table=DISTRICT,
                col=(D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE, D_ZIP,D_YTD),
                where=(D_ID,eq,d_id))
            if res == SQLState.ABORT:
                return SQLState.ABORT 
            d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd = res[0]
        except Exception as e:
            d_name = 'null'
        # d_ytd = eval(d_ytd)
        if update(client=self._client,
               table=DISTRICT,
               row=(D_YTD,D_YTD+'+'+str(h_amount)),
               where=[(D_W_ID,eq,w_id),(D_ID,eq,d_id)]) == SQLState.ABORT:
            return SQLState.ABORT
        
        if(type(c_query) == str):
            try:

                result = select(client=self._client,
                                table=CUSTOMER,
                                col=(C_ID,C_FIRST,C_MIDDLE,C_LAST,C_STREET_1,C_STREET_2,C_CITY,C_STATE,
                                            C_ZIP,C_PHONE,C_SINCE,C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,
                                            C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT),
                                where=[(C_LAST,eq,c_query),
                                    (C_W_ID,eq,c_w_id),
                                    (C_D_ID,eq,c_d_id)],
                                # order_by=C_FIRST,
                                asc=True)
                if result == SQLState.ABORT:
                    return SQLState.ABORT
                result = result[0]
            except Exception as e:
                c_credit = 'GC'
                c_id = 1; c_balance = 0; c_ytd_payment = 0; c_payment_cnt = 0
        else:
            try:
                result = select(client=self._client,
                            table=CUSTOMER,
                            col=(C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
                                 C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
                                 C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT),
                            where=[(C_ID, eq, c_query),
                                   (C_W_ID, eq, c_w_id),
                                   (C_D_ID, eq, c_d_id)])
                if result == SQLState.ABORT:
                    return SQLState.ABORT
                result = result[0]
                c_id, c_first, c_midele, c_last, \
                c_street_1,c_street_2,c_city,c_state,\
                c_zip,c_phone,c_since,\
                c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt = result  # result[len(result)//2]
                c_id = eval(c_id); c_balance = eval(c_balance); c_ytd_payment = eval(c_ytd_payment); c_payment_cnt = eval(c_payment_cnt)
            except Exception as e:
                c_credit = 'GC'
                c_id = 1; c_balance = 0; c_ytd_payment = 0; c_payment_cnt = 0
        if update(client=self._client,
               table=CUSTOMER,
               row=[(C_BALANCE,c_balance+h_amount),
                    (C_YTD_PAYMENT,c_ytd_payment+1),
                    (C_PAYMENT_CNT,c_payment_cnt+1)],
               where=[(C_W_ID,eq,w_id),(C_D_ID,eq,d_id),(C_ID,eq,c_id)]) == SQLState.ABORT:
            return SQLState.ABORT
        if c_credit == 'BC':
            try:
                c_data = (''.join(map(str,[c_id,c_d_id,c_w_id,d_id,h_amount])) \
                            + select(client=self._client,
                                    table=CUSTOMER,
                                    col=C_DATA,
                                    where=[(C_ID,eq,c_id),
                                            (C_W_ID,eq,c_w_id),
                                            (C_D_ID,eq,c_d_id)])[0][0] )[0:500]
            except Exception as e:
                c_data = 'null'
            if update(client=self._client,
                   table=CUSTOMER,
                   row=(C_DATA,"'"+c_data+"'"),
                   where=[(C_W_ID,eq,w_id),(C_D_ID,eq,d_id),(C_ID,eq,c_id)]) == SQLState.ABORT:
                return SQLState.ABORT
                
        #4 blank space
        h_data = w_name + '    ' + d_name
        if insert(client=self._client,
               table=HISTORY,
               rows=(c_id,c_d_id,c_w_id,d_id,w_id,"'"+current_time()+"'",h_amount,"'"+h_data+"'")) == SQLState.ABORT:
            return SQLState.ABORT
        
        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- Payment')
        return SQLState.SUCCESS

    def do_order_status(self, w_id, d_id, c_query):
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ Order Status')
        if type(c_query) == str:
            try:
                result = select(client=self._client,
                                table=CUSTOMER,
                                col=(C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST),
                                where=[(C_LAST, eq, c_query),
                                    (C_W_ID, eq, w_id),
                                    (C_D_ID, eq, d_id)],
                                order_by=C_FIRST,
                                asc=True)
                if result == SQLState.ABORT:
                    return SQLState.ABORT
                result = result[0]
            except Exception as e:
                result = None
        else:
            try:
                result = select(client=self._client,
                            table=CUSTOMER,
                            col=(C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST),
                            where=[(C_ID, eq, c_query),
                                   (C_W_ID, eq, w_id),
                                   (C_D_ID, eq, d_id)])
                if result == SQLState.ABORT:
                    return SQLState.ABORT
                result = result[0]
                c_id, c_balance, c_first, c_middle, c_last = result  # result[len(result)//2]
                c_id = eval(c_id)
            except Exception as e:
                c_id = 2101
        try:
            res = select(client=self._client,
                        table=ORDERS,
                        col=(O_ID,O_ENTRY_D,O_CARRIER_ID),
                        where=[(O_W_ID, eq, w_id),
                                (O_D_ID, eq, d_id),
                                (O_C_ID, eq, c_id)],
                        # order_by=O_ID
                        )
            
            if res == SQLState.ABORT:
                return SQLState.ABORT
            o_id, o_entry_id, o_carrier_id = res[0]
            o_id = eval(o_id)
        except Exception as e:
            o_id = 1
        try:
            res = select(client=self._client,#ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d
                        table=ORDER_LINE,
                        col=(OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY,OL_AMOUNT,OL_DELIVERY_D),
                        where=[(OL_W_ID,eq,w_id),
                               (OL_D_ID,eq,d_id),
                               (OL_O_ID,eq,o_id)])
            if res == SQLState.ABORT:
                return SQLState.ABORT
            result = res[0]
        except Exception as e:
            result = None
        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- Order Status')
        return SQLState.SUCCESS


    def do_delivery(self, w_id, o_carrier_id):
        t1 = time.time()
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ Delivery')
        # dat = q.get()
        # w_id = dat['w_id']
        # o_carrier_id = dat['o_carrier_id']
        for d_id in range(1,11):
            try:
                res = select(client=self._client,
                            table=NEW_ORDERS,
                            col=NO_O_ID,
                            where=[(NO_W_ID,eq,w_id),(NO_D_ID,eq,d_id)],
                            # order_by=NO_O_ID,
                            asc=True)
                if res == SQLState.ABORT:
                    return SQLState.ABORT
                o_id = res[0][0]
                o_id = eval(o_id)
            except Exception as e:
                o_id = 2101
            if delete(client=self._client,
                    table=NEW_ORDERS,
                    where=[(NO_W_ID,eq,w_id),(NO_D_ID,eq,d_id),(NO_O_ID,eq,o_id)]) == SQLState.ABORT:
                return SQLState.ABORT
            try:
                res = select(client=self._client,
                                table=ORDERS,
                                col=O_C_ID,
                                where=[(O_ID,eq,o_id),(O_W_ID,eq,w_id),(O_D_ID,eq,d_id)])
                if res == SQLState.ABORT:
                    return SQLState.ABORT
                o_c_id = res[0][0]
                o_c_id = eval(o_c_id)
            except Exception as e:
                o_c_id = 2101
            if update(client=self._client,
                    table=ORDERS,
                    row=(O_CARRIER_ID,o_carrier_id),
                    where=[(O_ID,eq,o_id),(O_W_ID,eq,w_id),(O_D_ID,eq,d_id)]) == SQLState.ABORT:
                return SQLState.ABORT
            try:
                res = select(client=self._client,
                                        table=ORDER_LINE,
                                        where=[(OL_W_ID,eq,w_id),(OL_D_ID,eq,d_id),(OL_O_ID,eq,o_id)])
                if res == SQLState.ABORT:
                    return SQLState.ABORT
                order_lines = res
                if not order_lines:
                    order_lines = []
                else:
                    order_lines = [i.strip('|').split('|').strip('- ') for i in order_lines[1:]]

            except Exception as e:
                order_lines = []
            
            try:
                res = select(client=self._client,
                                    table=ORDER_LINE,
                                    col = OL_AMOUNT,
                                    where=[(OL_W_ID, eq, w_id), (OL_D_ID, eq, d_id), (OL_O_ID, eq, o_id)])
                if res == SQLState.ABORT:
                    return SQLState.ABORT
                ol_amount = res[0]
                if not ol_amount:
                    ol_amount = 0
                else:
                    ol_amount = [eval(o[0]) for o in ol_amount]
                    ol_amount = sum(ol_amount)
            except Exception as e:
                ol_amount = 0
            
            for line in order_lines:
                if line[0] == '':
                    continue
                if update(client=self._client,
                        table=ORDER_LINE,
                        row=(OL_DELIVERY_D,"'"+current_time()+"'"),
                        where=[(OL_W_ID,eq,w_id),(OL_D_ID,eq,d_id),(OL_O_ID,eq,eval(line[0]))]) == SQLState.ABORT:
                    return SQLState.ABORT
            try:
                res = select(client=self._client,
                            table=CUSTOMER,
                            col=(C_BALANCE,C_DELIVERY_CNT),
                            where=[(C_W_ID,eq,w_id),(C_D_ID,eq,d_id),(C_ID,eq,o_c_id)])
                if res == SQLState.ABORT:
                    return SQLState.ABORT
                c_balance,c_delivery_cnt = res[0]
                c_balance = eval(c_balance); c_delivery_cnt = eval(c_delivery_cnt)
            except Exception as e:
                c_balance = 0; c_delivery_cnt = 0
            #print(c_balance, ol_amount, c_delivery_cnt)
            if update(client=self._client,
                    table=CUSTOMER,
                    row=[(C_BALANCE,c_balance+ol_amount),(C_DELIVERY_CNT,c_delivery_cnt+1)],
                    where=[(C_W_ID,eq,w_id),(C_D_ID,eq,d_id),(C_ID,eq,o_c_id)]) == SQLState.ABORT:
                return SQLState.ABORT
        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        t2 = time.time()
        # put_txn(lock,Delivery,t2-t1,True)
        # print('- Delivery')

        self._delivery_stop = True
        return SQLState.SUCCESS


    def do_stock_level(self, w_id, d_id, level):
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ Stock Level')
        try:
            res = select(client=self._client,
                        table=DISTRICT,
                        col=D_NEXT_O_ID,
                        where=[(D_W_ID,eq,w_id),(D_ID,eq,d_id)])
            if res == SQLState.ABORT:
                return SQLState.ABORT
            d_next_o_id = res[0][0]
            d_next_o_id = eval(d_next_o_id)
        except Exception as e:
            d_next_o_id = 0
        try:
            res = select(client=self._client,
                        table=ORDER_LINE,
                        where=[(OL_W_ID,eq,w_id),
                                (OL_D_ID,eq,d_id),
                                (OL_O_ID,beq,d_next_o_id-20),
                                (OL_O_ID,lt,d_next_o_id)])
            if res == SQLState.ABORT:
                return SQLState.ABORT
            order_lines = res
            items = set([order_line[4] for order_line in order_lines])
        except Exception as e:
            items = []
        low_stock = 0
        for item in items:
            try:
                res = select(client=self._client,
                            table=STOCK,
                            col=S_QUANTITY,
                            where=[(S_I_ID,eq,item),
                                    (S_W_ID,eq,w_id),
                                    (S_QUANTITY,lt,level)])
                if res == SQLState.ABORT:
                    return SQLState.ABORT
                cur_quantity = res[0][0]
            except Exception as e:
                cur_quantity = 0
            # low_stock += eval(cur_quantity)
        # low_stock = select(client=self._client,
        #                     table=STOCK,
        #                     col=S_QUANTITY,
        #                     where=[(S_W_ID,eq,w_id),(S_I_ID,eq,ol_i_id),(S_QUANTITY,lt,level)])
        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- Stock Level')
        return SQLState.SUCCESS