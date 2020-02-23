#!/usr/bin/env python3
# coding=utf-8
"""模拟生产数据脚本"""
import time
import json
import struct
import random
import base64

from kafka import KafkaProducer

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    for i in range(162):
        image_body = {
            'image_id': i,
            'image_body': base64.b64encode(struct.pack('>I', i)).decode()
        }
        producer.send('imageBody', json.dumps(image_body).encode())
        time.sleep(0.01)
    producer.close()
    print('done.')
