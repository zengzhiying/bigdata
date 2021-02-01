#!/usr/bin/env python3
# coding=utf-8
"""clickhouse 游标测试
 CREATE TABLE default.flash
(
    `a` UInt32, 
    `t` DateTime
)
ENGINE = MergeTree()
ORDER BY t
SETTINGS index_granularity = 8192
"""
import time
from datetime import datetime

from clickhouse_driver import Client

if __name__ == '__main__':
    c = Client(host="192.168.122.5", port=9000, database='default')
    c2 = Client(host="192.168.122.5", port=9000, database='default')

    insert_sql = "INSERT INTO flash (a, t) VALUES"
    # rows = []
    # for i in range(100000):
    #     rows.append((i + 1, datetime.now()))

    # c.execute(insert_sql, rows)
    # print("insert ok!")

    settings = {'max_block_size': 1000}
    rows_gen = c.execute_iter("select * from flash", settings=settings)

    for row in rows_gen:
        print(row)
        # 插入 共用连接 error
        # c.execute(insert_sql, [(100001, datetime.now())])
        # 使用新的连接解决问题
        c2.execute(insert_sql, [(100001, datetime.now())])
        # sleep
        time.sleep(1)

