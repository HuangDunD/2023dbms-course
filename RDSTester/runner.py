from mysql.driver import Driver
from tester import do_test
from record.record import build_db
from record.record import analysis
from multiprocessing import Process, Lock
import numpy as np
import matplotlib.pyplot as plt
import os
import time
import shutil
import argparse

def clean():
    shutil.rmtree('RDSTester/result')
    os.mkdir('RDSTester/result')
    build_db()

def prepare():
    driver = Driver(scale=1)
    driver.build()  # 创建9个tables
    driver.create_index() # 建立除history表外其余表的索引
    driver.load()  # 加载csv数据到9张表
    driver.delay_close()

def test(lock, tid, txns=150, txn_prob=[0.45, 0.43, 0.04, 0.04, 0.04]):
    print(f'+ Test_{tid} Begin')
    driver = Driver(scale=1)
    do_test(driver, lock, txns, txn_prob)
    print(f'- Test_{tid} Finished')
    driver.delay_close()

def output_result():
    result ,new_order_result = analysis()
    f = open(f'RDSTester/result/statistics_of_five_transactions.txt', 'w')
    for r in result:
        f.write(str(r['name']+' - '+'\navg time: '+ str(r['avg'])+ '\ntotal: '+str(r['total'])+'\nsuccess: '+str(r['success'])+'\n\n'))
        print(r['name'],' - ','\navg time: ', r['avg'], '\ntotal: ',r['total'],'\nsuccess: ',r['success'])
    f2 = open(f'RDSTester/result/timecost_and_num_of_NewOrders.txt', 'w')
    for n in new_order_result:
        f2.write("number: " + str(n[0]) + ", time cost: " + str(n[1]) + "\n")    
    X = np.array([e[1] for e in new_order_result])
    Y = np.array([e[0] for e in new_order_result])
    plt.plot(X,Y)
    plt.ylabel('Number of New-Orders')
    plt.xlabel('Time unit: second')
    plt.savefig(f"RDSTester/result/timecost_and_num_of_NewOrders.jpg")
    plt.show()
    os.remove(f'RDSTester/result/rds.db')

# useage: python RDSTester/runner.py --prepare --thread 8 --rw 150 --ro 150 --analyze
def main():
    parser = argparse.ArgumentParser(description='Python Script with Thread Number Argument')
    parser.add_argument('--prepare', action='store_true', help='Enable prepare mode')
    parser.add_argument('--analyze', action='store_true', help='Enable analyze mode')
    parser.add_argument('--rw', type=int, help='Read write transaction phase time')
    parser.add_argument('--ro', type=int, help='Read only transaction phase time')
    parser.add_argument('--thread', type=int, help='Thread number')

    args = parser.parse_args()
    thread_num = args.thread

    clean()

    if args.prepare:
        lt1 = time.time()
        prepare()
        print(f'load time: {time.time() - lt1}')

    if thread_num:
        lock = Lock()
        t1 = time.time()
        process_list = []
        if args.rw:
            for i in range(thread_num):
                process_list.append(Process(target=test, args=(lock, i+1, args.rw, [10,10,1,1,1])))
                process_list[i].start()

            for i in range(thread_num):
                process_list[i].join()
        t2 = time.time()
        process_list = []
        if args.ro:
            for i in range(thread_num):
                process_list.append(Process(target=test, args=(lock, i+1, args.ro, [0,0,1,1,0])))
                process_list[i].start()

            for i in range(thread_num):
                process_list[i].join()
        t3 = time.time()
    if args.analyze:
        output_result()
        print(f'total time of rw txns: {t2-t1}')
        print(f'total time of ro txns: {t3-t2}')
        print(f'total time: {t3-t1}')

if __name__ == '__main__':
    main()