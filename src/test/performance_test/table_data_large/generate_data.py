import random
import csv
from datetime import datetime, timedelta

def gendata_order_line(num_rows):
    data = [['ol_o_id', 'ol_d_id', 'ol_w_id', 'ol_number', 'ol_i_id', 'ol_supply_w_id', 'ol_delivery_d', 'ol_quantity', 'ol_amount', 'ol_dist_info']]
    for i in range(1, num_rows + 1):
        ol_o_id = i
        ol_d_id = random.randint(1, 10)
        ol_w_id = random.randint(1, 5)
        ol_number = random.randint(1, 100)
        ol_i_id = random.randint(1, 1000)
        ol_supply_w_id = random.randint(1, 5)
        ol_delivery_d = datetime.now() - timedelta(days=random.randint(1, 365))
        ol_quantity = random.randint(1, 10)
        ol_amount = round(random.uniform(10, 1000), 2)
        ol_dist_info = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=23))
        row = (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, str(ol_delivery_d).split(".")[0], ol_quantity, ol_amount, ol_dist_info)
        data.append(row)

    save_to_csv(data, 'order_line.csv')
    print(f'{num_rows} rows have been generated and saved to "order_line.csv".')


def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerows(data)

if __name__ == '__main__':
    gendata_order_line(1000000)
