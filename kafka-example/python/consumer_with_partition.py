#!/usr/bin/env python3
# coding=utf-8
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition

# Kafka 配置
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

# 创建 Kafka 消费者实例
consumer = Consumer(conf)

# 指定要消费的 Topic 和分区
topic = 'my_topic'
partition_offsets = [(0, 11102), (2, 18912)]

# 分配特定的分区和偏移量
consumer.assign([TopicPartition(topic=topic, partition=p, offset=o) for p, o in partition_offsets])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # 到达分区的结束（EOF）
                print(f"End of partition reached: {msg.topic()} [{msg.partition}] @ {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            # 打印消费的消息
            print(f"Consumed message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()

