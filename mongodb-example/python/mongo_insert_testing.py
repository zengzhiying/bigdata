#!/usr/bin/env python3
# coding=utf-8
import time
import random

import numpy as np
from pymongo import MongoClient

def fill_column(doc):
    for i in range(doc['col_count']):
        vector = np.random.random((1, 128)).astype(np.float16)
        doc['col{}'.format(i + 1)] = vector.tobytes()

if __name__ == '__main__':
    client = MongoClient('mongodb://127.0.0.1:27017/')
    db = client.face

    docs = []
    starter = time.time()
    for i in range(100000):
        col_count = random.randint(7, 11)
        row_count = col_count << 4
        doc = {
            '_id': str(i + 1),
            'group': i + 1,
            'row_count': row_count,
            'col_count': col_count,
        }

        fill_column(doc)
        docs.append(doc)
        if len(docs) == 1000:
            # ordered: False表示每批不再等待上一个批次的LastError
            # 对集群来说可以提高分片并行度 性能较高
            res = db.groups.insert_many(docs, ordered=False)
            # print(res.inserted_ids)
            docs.clear()

    print(docs)
    print("time: {:.3f}s".format(time.time() - starter))
