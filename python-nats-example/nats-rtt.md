# nats_rtt.py 用法

## 🎯 主要功能

  1. RTT (往返时间) 测试

  - 使用 request-reply 模式测量延迟
  - 自动设置回声响应器
  - 支持多次测试和统计分析

  2. 完整的统计信息

  - 最小/最大/平均/中位数 RTT
  - 成功率统计
  - 标准差计算

  3. 灵活的认证支持

  - 用户名/密码认证
  - 令牌认证
  - 无认证连接


## 基础RTT测试

```shell
python nats_rtt.py
```

## 指定服务器和认证

```shell
python nats_rtt.py --server nats://$NATS_SERVER --user $USER --password $PASSWORD
# 简单测试
python nats_rtt.py --server nats://$NATS_SERVER --user $USER --password $PASSWORD --count 1
```

## 自定义测试参数

```shell
python nats_rtt.py -s nats://$NATS_SERVER -u $USER -p $PASSWORD -c 20 -i 0.5
```

其他高级选项如下参考后续的命令。

### 只显示连接信息

```shell
python nats_rtt.py --server nats://$NATS_SERVER --user $USER --password $PASSWORD --info
```

### 自定义测试参数

```shell
python nats_rtt.py \
    --server nats://$NATS_SERVER \
    --user $USER \
    --password $PASSWORD \
    --count 50 \
    --interval 0.2 \
    --timeout 5.0 \
    --subject custom.rtt.test
```

📊 输出示例

```
🔗 NATS RTT 测试
   服务器: nats://xx.xx.xx.xx:4222
   用户: <user>
   主题: nats.rtt.test
   测试次数: 10
   间隔: 1.0s
   超时: 2.0s

🏓 RTT 测试 # 1 时间= 15.32ms
🏓 RTT 测试 # 2 时间= 12.85ms
  ...

==================================================
📊 RTT 测试统计
==================================================
总测试次数: 10
成功次数:   10
失败次数:   0
成功率:     100.0%

RTT 统计 (毫秒):
  最小值:   11.25ms
  最大值:   18.67ms
  平均值:   14.52ms
  中位数:   14.12ms
  标准差:   2.34ms
```

### 🔧 在代码中使用

```python
import asyncio
from nats_rtt import quick_rtt_test
# ⚡ 快速测试连通性
stats = await quick_rtt_test(
    server_url="nats://192.168.1.17:4222",
    username="backend",
    password="123456",
    count=5
)
```
