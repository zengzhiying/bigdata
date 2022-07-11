#!/usr/bin/env python3
# coding: utf-8
"""milvus 相似计算测试代码
milvus版本: 2.0.2
"""
import sys
import time
import random

import numpy
from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)

def unitization(f):
    return f / numpy.linalg.norm(f)

if __name__ == '__main__':
    connections.connect("default", host="localhost", port="19530")
    if not utility.has_collection("voice"):
        fields = [
            FieldSchema(name="voice_id", dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name="age", dtype=DataType.INT64),
            FieldSchema(name="voice_vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        schema = CollectionSchema(fields, "voice voice_vector search.")
        voice = Collection("voice", schema)
    else:
        voice = Collection("voice")

    print(utility.list_collections())

    if len(sys.argv) == 2:
        # 测试增量索引
        print(voice.load())
        # 插入5条数据
        entities = [
            [100000001, 100000002, 100000003, 100000004, 100000005],  # field voice_id
            [6, 8, 10, 3, 7],  # field age
            [list(unitization(numpy.random.random(128))), list(unitization(numpy.random.random(128))), list(unitization(numpy.random.random(128))), 
             list(unitization(numpy.random.random(128))), list(unitization(numpy.random.random(128)))],  # field voice_vector
        ]

        print(voice.insert(entities))
        print("写入完毕.")

        vectors_to_search = entities[-1][:2]

        # 条件检索
        print(voice.query(expr="voice_id in [100000002, 100000003]", output_fields=['voice_id', 'age']))
        # 比对
        search_params = {
            "metric_type": "IP",
            "params": {"nprobe": 200},
        }

        start_time = time.time()
        result = voice.search(vectors_to_search, "voice_vector", search_params, limit=20, output_fields=["voice_id"])

        end_time = time.time()
        print("Search IVF FLAT用时: {}s".format(end_time - start_time))
        # 注意: 打印只打印前10条  使用: result[0].ids, result[0].distances
        # def __str__(self):
        #    return str(list(map(str, self.__getitem__(slice(0, 10)))))
        print(result)
        

        # 增量索引
        index = {
            "index_type": "IVF_FLAT",
            "metric_type": "IP",
            "params": {"nlist": 256},
        }

        voice.create_index("voice_vector", index)

        print("增量索引完成!")

        print(voice.load())
        print("增量加载完成!")

        # 条件检索
        print(voice.query(expr="voice_id in [100000002, 100000003]", output_fields=['voice_id', 'age']))
        # 比对
        search_params = {
            "metric_type": "IP",
            "params": {"nprobe": 200},
        }

        start_time = time.time()
        result = voice.search(vectors_to_search, "voice_vector", search_params, limit=20, output_fields=["voice_id"])

        end_time = time.time()
        print("Search IVF FLAT用时: {}s".format(end_time - start_time))
        print(result)

        connections.disconnect('default')

        sys.exit(0)



    # 写入数据
    batch_size = 10000

    entities = [
        [],  # field voice_id
        [],  # field age
        [],  # field voice_vector
    ]

    for i in range(100000000):
        age = random.randint(1, 8)
        voice_vector = unitization(numpy.random.random(128))

        entities[0].append(i + 1)
        entities[1].append(age)
        entities[2].append(list(voice_vector))
        if len(entities[0]) == batch_size:
            insert_result = voice.insert(entities)
            print("insert %d" % (i + 1))
            entities = [[], [], []]

    print("完成写入！")

    voice.load()

    # 1.条件检索
    result = voice.query(expr="voice_id in [22, 10022, 666]", output_fields=['voice_id', 'age', 'voice_vector'])
    print(result)

    # 2.穷举搜索
    vector = unitization(numpy.random.random(128))
    vectors = [list(vector)]

    search_params = {"metric_type": "IP",}
    start_time = time.time()
    
    result = voice.search(vectors, "voice_vector", search_params, limit=20, output_fields=["voice_id"])
    end_time = time.time()
    print("Search FLAT用时: {}s".format(end_time - start_time))
    print(result)

    voice.release()


    # 创建索引
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "IP",
        "params": {"nlist": 256},
    }

    voice.create_index("voice_vector", index)

    print("创建索引完成!")


    voice.load()

    search_params = {
        "metric_type": "IP",
        "params": {"nprobe": 100},
    }

    start_time = time.time()
    result = voice.search(vectors, "voice_vector", search_params, limit=20, output_fields=["voice_id"])

    end_time = time.time()
    print("Search IVF FLAT用时: {}s".format(end_time - start_time))
    print(result)


    voice.release()

    connections.disconnect('default')



