#!/usr/bin/env python3
# coding=utf-8
"""模拟生产数据脚本"""
import time
import json
import random

from kafka import KafkaProducer

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    for i in range(10000):
        image_label = {
            'label_id': random.randint(0, 36),
            'label_name': f'house: {i}'
        }
        producer.send('imageLabel', json.dumps(image_label).encode())
        time.sleep(0.01)
    producer.close()
    print('done.')
