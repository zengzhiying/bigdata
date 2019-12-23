#!/usr/bin/env python3
# coding=utf-8
import time

import redis

stream_name = 'mystream'
group_name = 't-group'
consumer_name = 'c1'

poll_batch = 10

if __name__ == '__main__':
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    groups = r.xinfo_groups(stream_name)
    for group_info in groups:
        if group_info['name'].decode('utf-8') == group_name:
            print("group: {} 已存在!".format(group_name))
            break
    else:
        # $ - 最新, 0-0 - 最早
        s = r.xgroup_create(stream_name, group_name, id='$', mkstream=True)
        print("created {}".format(s))

    number_poll = 0
    while True:
        # 默认非阻塞, 0 - 阻塞, > 0 超时时间, 单位: ms
        messages = r.xreadgroup(group_name,
                                consumer_name,
                                {stream_name: '>'},
                                count=poll_batch,
                                block=1000)
        if messages:
            msgs = messages[0][1]
            # print(f"number: {len(msgs)}")
            ack_ids = []
            for msg_id, msg_value in msgs:
                print(msg_id, msg_value)
                # 处理消息

                # 单次 确认消息
                # ok = r.xack(stream_name, group_name, msg_id)
                # print(f"ack: {ok}")
                ack_ids.append(msg_id)
                number_poll += 1
            # 统一确认消息
            if ack_ids:
                ok = r.xack(stream_name, group_name, *ack_ids)
                print(f"batch ack: {ok}, number: {number_poll}")
