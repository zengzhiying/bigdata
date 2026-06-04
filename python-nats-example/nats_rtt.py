#!/usr/bin/env python3
"""
NATS RTT (Round Trip Time) 测试工具
用于测试 NATS 服务器的连通性和响应时间
"""

import asyncio
import time
import statistics
import argparse
from datetime import datetime
from typing import List, Optional

import nats
from nats.errors import TimeoutError, NoServersError


class NATSRTTTester:
    """NATS RTT 测试器"""

    def __init__(
        self,
        server_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        connect_timeout: float = 5.0
    ):
        self.server_url = server_url
        self.username = username
        self.password = password
        self.token = token
        self.connect_timeout = connect_timeout
        self.nc: Optional[nats.NATS] = None

    async def connect(self) -> bool:
        """连接到 NATS 服务器"""
        try:
            connect_kwargs = {
                "servers": [self.server_url],
                "connect_timeout": self.connect_timeout,
                "allow_reconnect": False,  # RTT测试不需要重连
            }

            # 设置认证
            if self.username and self.password:
                connect_kwargs["user"] = self.username
                connect_kwargs["password"] = self.password
            elif self.token:
                connect_kwargs["token"] = self.token

            self.nc = await nats.connect(**connect_kwargs)
            return True

        except Exception as e:
            print(f"❌ 连接失败: {e}")
            return False

    async def disconnect(self):
        """断开连接"""
        if self.nc:
            await self.nc.close()
            self.nc = None

    async def single_rtt(self, subject: str = "nats.rtt.test", timeout: float = 2.0) -> Optional[float]:
        """执行单次RTT测试"""
        if not self.nc:
            return None

        try:
            start_time = time.perf_counter()

            # 使用 request-reply 模式测试 RTT
            response = await self.nc.request(
                subject,
                b"ping",
                timeout=timeout
            )

            end_time = time.perf_counter()
            rtt_ms = (end_time - start_time) * 1000  # 转换为毫秒

            return rtt_ms

        except TimeoutError:
            print(f"⏰ RTT 测试超时 (>{timeout}s)")
            return None
        except Exception as e:
            print(f"❌ RTT 测试失败: {e}")
            return None

    async def setup_echo_responder(self, subject: str = "nats.rtt.test"):
        """设置回声响应器"""
        async def echo_handler(msg):
            # 简单回复相同的消息
            await msg.respond(msg.data)

        await self.nc.subscribe(subject, cb=echo_handler)

    async def rtt_test(
        self,
        count: int = 10,
        interval: float = 1.0,
        subject: str = "nats.rtt.test",
        timeout: float = 2.0,
        setup_responder: bool = True
    ) -> dict:
        """执行完整的RTT测试"""

        print(f"🔗 NATS RTT 测试")
        print(f"   服务器: {self.server_url}")
        if self.username:
            print(f"   用户: {self.username}")
        print(f"   主题: {subject}")
        print(f"   测试次数: {count}")
        print(f"   间隔: {interval}s")
        print(f"   超时: {timeout}s")
        print("=" * 50)

        # 设置回声响应器（如果需要）
        if setup_responder:
            await self.setup_echo_responder(subject)
            # 等待订阅生效
            await asyncio.sleep(0.1)

        rtts: List[float] = []
        successful_tests = 0
        failed_tests = 0

        for i in range(1, count + 1):
            print(f"🏓 RTT 测试 #{i:2d} ", end="", flush=True)

            rtt = await self.single_rtt(subject, timeout)

            if rtt is not None:
                rtts.append(rtt)
                successful_tests += 1
                print(f"时间={rtt:6.2f}ms")
            else:
                failed_tests += 1
                print("失败")

            # 等待间隔（最后一次测试不等待）
            if i < count:
                await asyncio.sleep(interval)

        # 计算统计信息
        stats = self._calculate_stats(rtts, successful_tests, failed_tests)
        self._print_stats(stats)

        return stats

    def _calculate_stats(self, rtts: List[float], successful: int, failed: int) -> dict:
        """计算RTT统计信息"""
        total_tests = successful + failed

        stats = {
            "total_tests": total_tests,
            "successful": successful,
            "failed": failed,
            "success_rate": (successful / total_tests * 100) if total_tests > 0 else 0,
            "rtts": rtts
        }

        if rtts:
            stats.update({
                "min_rtt": min(rtts),
                "max_rtt": max(rtts),
                "avg_rtt": statistics.mean(rtts),
                "median_rtt": statistics.median(rtts),
                "stddev_rtt": statistics.stdev(rtts) if len(rtts) > 1 else 0.0
            })

        return stats

    def _print_stats(self, stats: dict):
        """打印统计信息"""
        print("\n" + "=" * 50)
        print("📊 RTT 测试统计")
        print("=" * 50)
        print(f"总测试次数: {stats['total_tests']}")
        print(f"成功次数:   {stats['successful']}")
        print(f"失败次数:   {stats['failed']}")
        print(f"成功率:     {stats['success_rate']:.1f}%")

        if stats['rtts']:
            print(f"\nRTT 统计 (毫秒):")
            print(f"  最小值:   {stats['min_rtt']:.2f}ms")
            print(f"  最大值:   {stats['max_rtt']:.2f}ms")
            print(f"  平均值:   {stats['avg_rtt']:.2f}ms")
            print(f"  中位数:   {stats['median_rtt']:.2f}ms")
            if len(stats['rtts']) > 1:
                print(f"  标准差:   {stats['stddev_rtt']:.2f}ms")

    async def connection_info(self):
        """显示连接信息"""
        if not self.nc:
            print("❌ 未连接到NATS服务器")
            return

        print("📋 NATS 连接信息")
        print("=" * 30)
        print(f"服务器URL:    {self.nc.connected_url}")
        print(f"客户端ID:     {self.nc.client_id}")
        print(f"连接状态:     {'已连接' if self.nc.is_connected else '未连接'}")

        # 获取服务器统计信息
        stats = self.nc.stats
        print(f"\n📊 连接统计:")
        print(f"  发送消息数: {stats['out_msgs']}")
        print(f"  接收消息数: {stats['in_msgs']}")
        print(f"  发送字节数: {stats['out_bytes']}")
        print(f"  接收字节数: {stats['in_bytes']}")
        print(f"  重连次数:   {stats['reconnects']}")


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="NATS RTT 测试工具")
    parser.add_argument("--server", "-s", default="nats://localhost:4222",
                       help="NATS 服务器地址")
    parser.add_argument("--user", "-u", help="用户名")
    parser.add_argument("--password", "-p", help="密码")
    parser.add_argument("--token", "-t", help="认证令牌")
    parser.add_argument("--count", "-c", type=int, default=10,
                       help="测试次数")
    parser.add_argument("--interval", "-i", type=float, default=1.0,
                       help="测试间隔（秒）")
    parser.add_argument("--subject", default="nats.rtt.test",
                       help="测试主题")
    parser.add_argument("--timeout", type=float, default=2.0,
                       help="单次测试超时时间（秒）")
    parser.add_argument("--connect-timeout", type=float, default=5.0,
                       help="连接超时时间（秒）")
    parser.add_argument("--info", action="store_true",
                       help="只显示连接信息")
    parser.add_argument("--no-responder", action="store_true",
                       help="不设置回声响应器（需要服务器端有响应器）")

    args = parser.parse_args()

    # 创建RTT测试器
    tester = NATSRTTTester(
        server_url=args.server,
        username=args.user,
        password=args.password,
        token=args.token,
        connect_timeout=args.connect_timeout
    )

    try:
        # 连接到服务器
        print(f"🔗 连接到 NATS 服务器: {args.server}")
        if not await tester.connect():
            return 1

        print("✅ 连接成功!")

        # 如果只需要显示连接信息
        if args.info:
            await tester.connection_info()
        else:
            # 执行RTT测试
            await tester.rtt_test(
                count=args.count,
                interval=args.interval,
                subject=args.subject,
                timeout=args.timeout,
                setup_responder=not args.no_responder
            )

        return 0

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断测试")
        return 1
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        return 1
    finally:
        # 断开连接
        await tester.disconnect()


# 便捷函数
async def quick_rtt_test(
    server_url: str = "nats://localhost:4222",
    username: Optional[str] = None,
    password: Optional[str] = None,
    count: int = 5
):
    """快速RTT测试"""
    tester = NATSRTTTester(server_url, username, password)

    try:
        if await tester.connect():
            print("✅ 连接成功，开始RTT测试...")
            stats = await tester.rtt_test(count=count, interval=0.5)
            return stats
        else:
            print("❌ 连接失败")
            return None
    finally:
        await tester.disconnect()


if __name__ == "__main__":
    import sys

    # 如果没有安装 nats-py，提示安装
    try:
        import nats
    except ImportError:
        print("❌ 缺少依赖包，请安装:")
        print("pip install nats-py")
        sys.exit(1)

    # 运行主程序
    exit_code = asyncio.run(main())
    sys.exit(exit_code)