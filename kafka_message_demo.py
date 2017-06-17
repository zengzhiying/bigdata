#!/usr/bin/env python
# coding=utf-8
import sys
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == '__main__':
    consumer_topic = "consumerMessage"
    producer_topic = "producerMessage"
    kafka_broker_list = "host1:9092,host2:9092,host3:9092"
    # 实例化kafka消费者客户端
    # 从最开始消费在参数列表添加 auto_offset_reset='earliest' 默认不加是实时/从上次断开消费
    consumer = KafkaConsumer(consumer_topic, group_id="test_group20170613", bootstrap_servers=kafka_broker_list)
    # 实例化生产者客户端
    producer = KafkaProducer(bootstrap_servers=kafka_broker_list)
    # 开始消费
    count = 0
    for msg in consumer:
        # 获取消息内容 msg.value
        message = json.loads(msg.value)
        # 其他处理...
        # ...
        # 推到新的topic
        producer.send(producer_topic, json.dumps(message, skipkeys=True))
        count += 1
        if count % 10000 == 0:
            print("条数: %d" % count)
            print("偏移量: %d" % msg.offset)

