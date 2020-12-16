#!/usr/bin/env python3
# coding=utf-8
import time
import pprint

from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient('mongodb://127.0.0.1:27018/')
    db = client.face

    r = db.groups.find()

    count = 0
    starter = time.time()
    for doc in r:
        # pprint.pprint(doc)
        # break
        count += 1
        if count % 10000 == 0:
            print(count)

    print("read docs time: {:.3f}s".format(time.time() - starter))
    print("count:", count)
    # 60000 rows/s

