import sqlite3
NewOrder = 0
Payment = 1
OrderStatus = 2
StockLevel = 3
Delivery = 4
name = {NewOrder:'New Order',Payment:'Payment',OrderStatus:'OrderStatus',StockLevel:'StockLevel',Delivery:'Delivery'}

def build_db():
    conn = sqlite3.connect(f'RDSTester/result/rds.db')
    cursor = conn.cursor()
    cursor.execute('create table new_order_txn(no integer, time real);')
    cursor.execute('create table test_result(txn integer, avg real, total integer, success integer);')
    cursor.executemany('insert into test_result(txn, avg, total, success) values(?,?,?,?);',
                    [(NewOrder,0,0,0),(Payment,0,0,0),(OrderStatus,0,0,0),(StockLevel,0,0,0),(Delivery,0,0,0)])
    cursor.execute('insert into new_order_txn(no, time) values(?,?);',(0,0))
    conn.commit()
    conn.close()

def put_new_order(lock, time):
    lock.acquire()
    conn = sqlite3.connect(f'RDSTester/result/rds.db')
    cursor = conn.cursor()
    cursor.execute('begin transaction;')
    cursor.execute('select no from new_order_txn order by no desc;')
    no = cursor.fetchone()[0]
    cursor.execute('insert into new_order_txn(no,time) values(?,?);', (no+1,time))
    conn.commit()
    conn.close()
    lock.release()


def put_txn(lock, txn, time, success):
    lock.acquire()
    conn = sqlite3.connect(f'RDSTester/result/rds.db')
    cursor = conn.cursor()
    cursor.execute('begin transaction;')
    cursor.execute('select avg, total, success from test_result where txn = ?',(txn,))
    avg, total, success_ = cursor.fetchone()
    if not success:
        success = 0
    cursor.execute('update test_result set avg = ?,total = ?, success = ? where txn=?;',(avg+time,total+1,success_+success,txn))
    conn.commit()
    conn.close()
    lock.release()


def analysis():
    conn = sqlite3.connect(f'RDSTester/result/rds.db')
    cursor = conn.cursor()
    cursor.execute('select * from test_result;')
    rows = cursor.fetchall()
    result = [{} for i in range(5)]
    for row in rows:
        if row[2] != 0:
            result[row[0]]['avg'] = row[1]/row[2]
        else:
            result[row[0]]['avg'] = 0
        result[row[0]]['total'] = row[2]
        result[row[0]]['success'] = row[3]
        result[row[0]]['name'] = name[row[0]]
    cursor.execute('select * from new_order_txn;')
    new_order_result = cursor.fetchall()
    conn.close()
    return result,new_order_result