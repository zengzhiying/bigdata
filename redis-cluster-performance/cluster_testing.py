#!/usr/bin/env python3
# coding=utf-8
"""辅助redis-benchmark测试redis客户端代码的性能
"""
import time

from rediscluster import RedisCluster

if __name__ == '__main__':
    nodes = [{"host": "192.168.122.5", "port": "6379"},
             {"host": "192.168.122.6", "port": "6379"},
             {"host": "192.168.122.6", "port": "6379"}]

    rc = RedisCluster(startup_nodes=nodes, decode_responses=False)


    # random set 100000
    starter = time.time()
    for i in range(100000):
        key = 'k_%d' % i
        value = 'v_%d' % i
        rc.set(key, value)
    interval = time.time() - starter
    print("set time: {:.3f}s, {:.2f} requests per second.".format(interval, 100000/interval))


    # random get 100000
    starter = time.time()
    for i in range(100000):
        key = 'k_%d' % i
        rc.get(key)
    interval = time.time() - starter
    print("get time: {:.3f}s, {:.2f} requests per second.".format(interval, 100000/interval))

    # random get 100000 Non-existent key
    starter = time.time()
    for i in range(100000):
        key = 'k_%d_non' % i
        rc.get(key)
    interval = time.time() - starter
    print("get non time: {:.3f}s, {:.2f} requests per second.".format(interval, 100000/interval))

    # lpush 
    push_key = 'mylist'
    starter = time.time()
    for i in range(100000):
        rc.lpush(push_key, str(i))
    interval = time.time() - starter
    print("lpush time: {:.3f}s, {:.2f} requests per second.".format(interval, 100000/interval))

    # lpop
    push_key = 'mylist'
    starter = time.time()
    for i in range(100000):
        rc.lpop(push_key)
    interval = time.time() - starter
    print("lpop time: {:.3f}s, {:.2f} requests per second.".format(interval, 100000/interval))

