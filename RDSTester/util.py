import random
import string
import datetime
_names = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']
_C_LOAD = 117
_C_RUN = 191
def rand_str(lower , upper = 0):
    if upper == 0 : upper = lower+1
    return ''.join([random.choice(string.ascii_letters) for i in range(random.randrange(lower,upper))])


def rand_dat(lower, upper):
    if random.randrange(100) < 10:
        s = rand_str(lower, upper-8)
        k = random.randrange(lower, upper-8)
        return s[lower:k]+'ORIGINAL'+s[k:upper-8]
    else:
        return rand_str(lower,upper)


def rand_digit(num):
    return ''.join([random.choice(string.digits) for i in range(num)])


def zip_code():
    rand_digit(4)+'11111'


def rand_perm(max):
    l = list(range(max))
    random.shuffle(l)
    return l


def NURand(A, x, y, C):
    return (((random.randrange(0,A)|random.randrange(x,y))+C) % (y-x)) + x   #y-1 = y


def get_c_last(k = 1000, run = False):
    C = _C_RUN if run else _C_LOAD
    if k >= 1000:
        k = NURand(255, 1, 1001, C)
    return ''.join([_names[k // 100], _names[(k // 10) % 10], _names[k % 10]])


def current_time():
    return str(datetime.datetime.now())[:19]


def get_c_id():
    return NURand(1023, 1, 3001, C=_C_RUN)


def get_ol_i_id():
    ol_cnt = random.randrange(5,16)
    rbk = random.randrange(100)
    ret = [NURand(8191, 1, 100001, C=_C_RUN) for i in range(ol_cnt)]
    if rbk == 0:
        ret[-1] = 100001# unused item number
    return ret


def get_ol_supply_w_id(home_w_id, scale, ol_cnt):
    supply_id = lambda: home_w_id if random.randrange(100) > 0 or scale == 1 else random.choice(
        list(range(scale)).remove(home_w_id))
    return [supply_id() for i in range(ol_cnt)]


def get_ol_quantity(ol_cnt):
    return [random.randrange(1,11) for i in range(ol_cnt)]


def get_d_id():
    return random.randrange(1,11)


def get_c_w_id_d_id(home_w_id,d_id, scale):
    c_w_id,c_d_id =  (home_w_id,d_id) \
                     if random.randrange(100) < 85 or scale == 1\
                     else (random.choice(list(range(1,scale+1)).remove(home_w_id)) , random.randrange(1,11))
    return c_w_id, c_d_id


def query_cus_by():
    # y = random.randrange(100)
    # if y < 60:
    #     return get_c_last(1000, run=True)
    # else:
    #     return get_c_id()
    return get_c_id()


def get_h_amount():
    return round(random.random() * (5000 - 1) +1 ,2)

def get_o_carrier_id():
    return random.randrange(1,11)
