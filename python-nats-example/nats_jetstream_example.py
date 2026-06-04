#!/usr/bin/env python3
"""
NATS JetStream 消息发布和消费示例
使用 Pull 方式消费消息，指定持久消费者名称
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Optional

import nats
from nats.errors import TimeoutError
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, AckPolicy


class NATSJetStreamClient:
    """NATS JetStream 客户端封装类"""

    def __init__(
        self,
        server_url: str = "nats://localhost:4222",
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        user_credentials: Optional[str] = None,
        nkey_file: Optional[str] = None,
        connect_timeout: float = 10.0,
        max_reconnect_attempts: int = 10
    ):
        """
        初始化 NATS JetStream 客户端

        Args:
            server_url: NATS 服务器地址
            username: 用户名（用于用户名/密码认证）
            password: 密码（用于用户名/密码认证）
            token: 认证令牌（用于令牌认证）
            user_credentials: 用户凭证文件路径（用于JWT认证）
            nkey_file: NKey文件路径（用于NKey认证）
            connect_timeout: 连接超时时间（秒）
            max_reconnect_attempts: 最大重连尝试次数
        """
        self.server_url = server_url
        self.username = username
        self.password = password
        self.token = token
        self.user_credentials = user_credentials
        self.nkey_file = nkey_file
        self.connect_timeout = connect_timeout
        self.max_reconnect_attempts = max_reconnect_attempts

        self.nc: Optional[nats.NATS] = None
        self.js: Optional[JetStreamContext] = None

    async def connect(self):
        """连接到 NATS 服务器"""
        try:
            # 构建连接参数
            connect_kwargs = {
                "servers": [self.server_url],
                "connect_timeout": self.connect_timeout,
                "max_reconnect_attempts": self.max_reconnect_attempts,
                "allow_reconnect": True,
                "ping_interval": 120,
                "max_outstanding_pings": 2
            }

            # 根据不同认证方式设置参数
            auth_method = "无认证"

            if self.username and self.password:
                # 用户名/密码认证
                connect_kwargs["user"] = self.username
                connect_kwargs["password"] = self.password
                auth_method = f"用户名/密码认证 (用户: {self.username})"

            elif self.token:
                # 令牌认证
                connect_kwargs["token"] = self.token
                auth_method = "令牌认证"

            elif self.user_credentials:
                # JWT 用户凭证认证
                connect_kwargs["user_credentials"] = self.user_credentials
                auth_method = f"JWT凭证认证 (文件: {self.user_credentials})"

            elif self.nkey_file:
                # NKey 认证
                connect_kwargs["nkeys_seed"] = self.nkey_file
                auth_method = f"NKey认证 (文件: {self.nkey_file})"

            print(f"🔗 正在连接 NATS 服务器...")
            print(f"   服务器: {self.server_url}")
            print(f"   认证方式: {auth_method}")

            # 连接到 NATS 服务器
            self.nc = await nats.connect(**connect_kwargs)

            # 设置连接事件处理器
            async def disconnected_cb():
                print("⚠️  与 NATS 服务器断开连接")

            async def reconnected_cb():
                print("🔄 已重新连接到 NATS 服务器")

            async def error_cb(e):
                print(f"❌ NATS 连接错误: {e}")

            async def closed_cb():
                print("🔌 NATS 连接已关闭")

            # 注册事件处理器
            self.nc._disconnected_cb = disconnected_cb
            self.nc._reconnected_cb = reconnected_cb
            self.nc._error_cb = error_cb
            self.nc._closed_cb = closed_cb

            # 初始化 JetStream 上下文
            self.js = self.nc.jetstream()

            print(f"✅ 成功连接到 NATS 服务器")
            print(f"   客户端ID: {self.nc.client_id}")
            print(f"   服务器信息: {self.nc.connected_url}")

        except Exception as e:
            print(f"❌ 连接 NATS 服务器失败: {e}")
            raise ConnectionError(f"无法连接到 NATS 服务器: {e}")

    async def disconnect(self):
        """断开 NATS 连接"""
        if self.nc:
            await self.nc.close()
            print("🔌 已断开 NATS 连接")

    async def publish_message(self, subject: str, message: dict, headers: dict = None):
        """
        发布消息到 JetStream

        Args:
            subject: 消息主题
            message: 消息内容（字典格式）
            headers: 消息头（可选）
        """
        if not self.js:
            raise Exception("JetStream 未初始化，请先连接")

        try:
            # 添加时间戳到消息
            message_with_timestamp = {
                **message,
                "timestamp": datetime.now().isoformat(),
                "message_id": f"msg_{int(time.time() * 1000)}"
            }

            # 转换为 JSON 字符串
            payload = json.dumps(message_with_timestamp, ensure_ascii=False)

            # 发布消息
            ack = await self.js.publish(
                subject=subject,
                payload=payload.encode('utf-8'),
                headers=headers
            )

            print(f"📤 消息发布成功:")
            print(f"   主题: {subject}")
            print(f"   序列号: {ack.seq}")
            print(f"   流: {ack.stream}")
            print(f"   消息内容: {message_with_timestamp}")

            return ack

        except Exception as e:
            print(f"❌ 发布消息失败: {e}")
            raise

    async def pull_consume_messages(
        self,
        stream_name: str,
        consumer_name: str,
        subject_filter: str = None,
        batch_size: int = 10,
        timeout: float = 5.0,
        max_messages: int = 100
    ):
        """
        使用 Pull 方式消费消息（持久消费者）

        Args:
            stream_name: 流名称
            consumer_name: 持久消费者名称
            subject_filter: 主题过滤器（可选）
            batch_size: 每次拉取的消息数量
            timeout: 拉取超时时间（秒）
            max_messages: 最大消费消息数量
        """
        if not self.js:
            raise Exception("JetStream 未初始化，请先连接")

        try:
            print(f"🔄 开始消费消息:")
            print(f"   流名称: {stream_name}")
            print(f"   消费者: {consumer_name}")
            print(f"   批次大小: {batch_size}")
            print(f"   超时时间: {timeout}s")
            print(f"   最大消息数: {max_messages}")
            print("-" * 50)

            consumed_count = 0

            # 获取持久消费者（假设已经创建）
            psub = await self.js.pull_subscribe(
                subject=subject_filter or "",
                durable=consumer_name,
                stream=stream_name
            )

            while consumed_count < max_messages:
                try:
                    # 拉取消息
                    remaining = min(batch_size, max_messages - consumed_count)
                    msgs = await psub.fetch(remaining, timeout=timeout)

                    if not msgs:
                        print("⏰ 没有新消息，继续等待...")
                        continue

                    # 处理消息
                    for msg in msgs:
                        consumed_count += 1
                        await self._process_message(msg, consumed_count)

                        # 确认消息处理完成
                        await msg.ack()

                except TimeoutError:
                    print("⏰ 拉取消息超时，继续尝试...")
                    continue
                except Exception as e:
                    print(f"❌ 消费消息时出错: {e}")
                    break

            print(f"🏁 消费完成，总共处理了 {consumed_count} 条消息")

        except Exception as e:
            print(f"❌ 消费消息失败: {e}")
            raise

    async def _process_message(self, msg, count: int):
        """
        处理单条消息

        Args:
            msg: NATS 消息对象
            count: 消息序号
        """
        try:
            # 解析消息内容
            payload = msg.data.decode('utf-8')
            message_data = json.loads(payload)

            # 获取消息元数据
            metadata = msg.metadata

            print(f"📨 消息 #{count}:")
            print(f"   主题: {msg.subject}")
            print(f"   流序列号: {metadata.sequence.stream}")
            print(f"   消费者序列号: {metadata.sequence.consumer}")
            print(f"   时间戳: {metadata.timestamp}")
            print(f"   重新投递次数: {metadata.num_delivered}")
            print(f"   消息内容: {message_data}")

            # 模拟消息处理逻辑
            await asyncio.sleep(0.1)  # 模拟处理时间

            print(f"✅ 消息 #{count} 处理完成")
            print("-" * 30)

        except json.JSONDecodeError as e:
            print(f"❌ 消息 #{count} JSON 解析失败: {e}")
        except Exception as e:
            print(f"❌ 消息 #{count} 处理失败: {e}")


async def main():
    """主函数 - 演示发布和消费消息"""

    # 配置参数
    NATS_URL = "nats://192.168.11.17:4222"
    STREAM_NAME = "test-stream"
    CONSUMER_NAME = "test-consumer"
    SUBJECT = "test.messages"

    # 认证配置（根据实际情况选择一种认证方式）
    AUTH_CONFIG = {
        # 选项1：用户名/密码认证
        "username": "backend",
        "password": "backend-123456",

        # 选项2：令牌认证（与用户名/密码二选一）
        # "token": "your_auth_token_here",

        # 选项3：JWT凭证认证（与其他方式二选一）
        # "user_credentials": "/path/to/user.creds",

        # 选项4：NKey认证（与其他方式二选一）
        # "nkey_file": "/path/to/user.nk",
    }

    # 创建客户端（带认证）
    client = NATSJetStreamClient(
        server_url=NATS_URL,
        **AUTH_CONFIG
    )

    try:
        # 连接到 NATS
        await client.connect()

        # 演示发布消息
        print("=" * 60)
        print("🚀 开始发布测试消息")
        print("=" * 60)

        # 发布多条测试消息
        test_messages = [
            {
                "type": "user_login",
                "user_id": "user_001",
                "action": "login",
                "ip": "192.168.1.100"
            },
            {
                "type": "order_created",
                "order_id": "order_12345",
                "user_id": "user_002",
                "amount": 99.99,
                "items": ["item1", "item2"]
            },
            {
                "type": "payment_completed",
                "order_id": "order_12345",
                "payment_method": "credit_card",
                "amount": 99.99
            },
            {
                "type": "system_alert",
                "level": "warning",
                "message": "磁盘空间不足",
                "server": "web-01"
            },
            {
                "type": "user_logout",
                "user_id": "user_001",
                "session_duration": 3600
            }
        ]

        for i, msg in enumerate(test_messages, 1):
            await client.publish_message(
                subject=SUBJECT,
                message=msg,
                headers={"message_type": msg["type"]}
            )
            await asyncio.sleep(0.5)  # 间隔发送

        print("\n" + "=" * 60)
        print("📥 开始消费消息")
        print("=" * 60)

        # 等待一下确保消息已经发布
        await asyncio.sleep(1)

        # 消费消息
        await client.pull_consume_messages(
            stream_name=STREAM_NAME,
            consumer_name=CONSUMER_NAME,
            subject_filter=SUBJECT,
            batch_size=2,
            timeout=3.0,
            max_messages=10
        )

    except Exception as e:
        print(f"❌ 程序执行出错: {e}")
    finally:
        # 断开连接
        await client.disconnect()


async def username_password_auth_example():
    """用户名/密码认证示例"""
    print("🔐 用户名/密码认证示例")

    client = NATSJetStreamClient(
        server_url="nats://localhost:4222",
        username="your_username",
        password="your_password"
    )

    try:
        await client.connect()

        # 发布测试消息
        await client.publish_message(
            subject="auth.test",
            message={
                "auth_type": "username_password",
                "message": "用户名/密码认证测试消息"
            }
        )

    finally:
        await client.disconnect()


async def token_auth_example():
    """令牌认证示例"""
    print("🎫 令牌认证示例")

    client = NATSJetStreamClient(
        server_url="nats://localhost:4222",
        token="your_auth_token_here"
    )

    try:
        await client.connect()

        # 发布测试消息
        await client.publish_message(
            subject="auth.test",
            message={
                "auth_type": "token",
                "message": "令牌认证测试消息"
            }
        )

    finally:
        await client.disconnect()


async def jwt_credentials_auth_example():
    """JWT凭证认证示例"""
    print("📜 JWT凭证认证示例")

    client = NATSJetStreamClient(
        server_url="nats://localhost:4222",
        user_credentials="/path/to/user.creds"  # JWT凭证文件路径
    )

    try:
        await client.connect()

        # 发布测试消息
        await client.publish_message(
            subject="auth.test",
            message={
                "auth_type": "jwt_credentials",
                "message": "JWT凭证认证测试消息"
            }
        )

    finally:
        await client.disconnect()


async def nkey_auth_example():
    """NKey认证示例"""
    print("🔑 NKey认证示例")

    client = NATSJetStreamClient(
        server_url="nats://localhost:4222",
        nkey_file="/path/to/user.nk"  # NKey种子文件路径
    )

    try:
        await client.connect()

        # 发布测试消息
        await client.publish_message(
            subject="auth.test",
            message={
                "auth_type": "nkey",
                "message": "NKey认证测试消息"
            }
        )

    finally:
        await client.disconnect()


async def multiple_servers_example():
    """多服务器连接示例"""
    print("🌐 多服务器连接示例")

    # 多个NATS服务器地址
    servers = [
        "nats://nats1.example.com:4222",
        "nats://nats2.example.com:4222",
        "nats://localhost:4222"
    ]

    # 将多个服务器地址用逗号连接
    server_url = ",".join(servers)

    client = NATSJetStreamClient(
        server_url=server_url,
        username="cluster_user",
        password="cluster_password",
        connect_timeout=15.0,
        max_reconnect_attempts=20
    )

    try:
        await client.connect()

        # 发布测试消息
        await client.publish_message(
            subject="cluster.test",
            message={
                "server_type": "cluster",
                "message": "多服务器连接测试消息"
            }
        )

    finally:
        await client.disconnect()


async def secure_connection_example():
    """安全连接示例（TLS）"""
    print("🔒 安全连接（TLS）示例")

    client = NATSJetStreamClient(
        server_url="tls://nats.example.com:4222",  # 使用 TLS 连接
        username="secure_user",
        password="secure_password",
        connect_timeout=20.0
    )

    try:
        await client.connect()

        # 发布测试消息
        await client.publish_message(
            subject="secure.test",
            message={
                "connection_type": "tls",
                "message": "安全连接测试消息"
            }
        )

    finally:
        await client.disconnect()


async def simple_publisher_example():
    """简单的发布者示例（无认证）"""
    print("📤 简单发布者示例（无认证）")

    client = NATSJetStreamClient()

    try:
        await client.connect()

        # 发布单条消息
        await client.publish_message(
            subject="simple.test",
            message={
                "event": "test_event",
                "data": "Hello NATS JetStream!",
                "priority": "high"
            }
        )

    finally:
        await client.disconnect()


async def simple_consumer_example():
    """简单的消费者示例（无认证）"""
    print("📥 简单消费者示例（无认证）")

    client = NATSJetStreamClient()

    try:
        await client.connect()

        # 消费消息
        await client.pull_consume_messages(
            stream_name="my-stream",
            consumer_name="my-consumer",
            subject_filter="simple.*",
            batch_size=5,
            max_messages=20
        )

    finally:
        await client.disconnect()


if __name__ == "__main__":
    print("🌟 NATS JetStream Python 客户端示例（支持多种认证方式）")
    print("=" * 60)

    # 运行主示例（默认使用用户名/密码认证）
    asyncio.run(main())

    # 如果想测试其他认证方式，可以注释掉上面的 main() 调用，
    # 然后取消注释下面的相应示例：

    # ==================== 认证方式示例 ====================

    # print("\n" + "=" * 60)
    # print("🔐 用户名/密码认证示例")
    # asyncio.run(username_password_auth_example())

    # print("\n" + "=" * 60)
    # print("🎫 令牌认证示例")
    # asyncio.run(token_auth_example())

    # print("\n" + "=" * 60)
    # print("📜 JWT凭证认证示例")
    # asyncio.run(jwt_credentials_auth_example())

    # print("\n" + "=" * 60)
    # print("🔑 NKey认证示例")
    # asyncio.run(nkey_auth_example())

    # print("\n" + "=" * 60)
    # print("🌐 多服务器连接示例")
    # asyncio.run(multiple_servers_example())

    # print("\n" + "=" * 60)
    # print("🔒 安全连接（TLS）示例")
    # asyncio.run(secure_connection_example())

    # ==================== 基础功能示例 ====================

    # print("\n" + "=" * 60)
    # print("📤 简单发布者示例")
    # asyncio.run(simple_publisher_example())

    # print("\n" + "=" * 60)
    # print("📥 简单消费者示例")
    # asyncio.run(simple_consumer_example())


# ==================== 认证配置说明 ====================
"""
认证配置选项说明：

1. 用户名/密码认证:
   client = NATSJetStreamClient(
       server_url="nats://localhost:4222",
       username="your_username",
       password="your_password"
   )

2. 令牌认证:
   client = NATSJetStreamClient(
       server_url="nats://localhost:4222",
       token="your_auth_token"
   )

3. JWT凭证认证:
   client = NATSJetStreamClient(
       server_url="nats://localhost:4222",
       user_credentials="/path/to/user.creds"
   )

4. NKey认证:
   client = NATSJetStreamClient(
       server_url="nats://localhost:4222",
       nkey_file="/path/to/user.nk"
   )

5. 多服务器连接:
   client = NATSJetStreamClient(
       server_url="nats://server1:4222,nats://server2:4222,nats://server3:4222",
       username="user",
       password="pass"
   )

6. TLS安全连接:
   client = NATSJetStreamClient(
       server_url="tls://nats.example.com:4222",
       username="user",
       password="pass"
   )

注意事项：
- 同时只能使用一种认证方式
- 确保NATS服务器配置了相应的认证方式
- JWT和NKey认证需要相应的凭证文件
- TLS连接需要服务器支持SSL/TLS
- 多服务器地址用逗号分隔
"""
