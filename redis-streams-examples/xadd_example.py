#!/usr/bin/env python3
# coding=utf-8
import time

import redis

stream_name = 'mystream'
maxlen = 1000000

if __name__ == '__main__':
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    for i in range(1000):
        s = r.xadd(stream_name, {'i': i}, id='*', maxlen=maxlen)
        print(s)
        time.sleep(0.1)
    print('done.')
