"""跨节点共享配额限速器 —— 全局虚拟时钟租约（batched GCRA）。

设计动机：Celery 自带的 ``rate_limit`` 是在**每个 worker 进程**内各建一个
kombu ``TokenBucket``（见 ``celery/worker/consumer/consumer.py`` 的
``bucket_for_task``），节点数 × 进程数会把总量放大。本模块把配额收归到
一个协调器（Redis）里，全集群共享同一条"发放时刻表"。

核心思想：配额不是"令牌库存"，而是"时间"。协调器只存一个单调时间戳
``next_us`` —— 下一个可用槽位的生效时刻。每次租约原子地把 ``next_us``
往后推 k 个间隔，返回这批槽位的时刻表；节点只按时刻表"开火"。

由此得到四个性质（对应分布式限流的四个经典坑）：

* 共享配额：所有节点共用同一个 key，总量 = rate，与节点数、进程数无关。
* 匀速放行：许可的生效时刻严格等距（1/rate），跨节点也等距；
  空闲期不"攒"额度（``next_us = max(next_us, now)``），不存在补发突发。
* 掉线归还：额度是时间不是库存。节点掉线后，它预约的未来槽位自然过期
  作废，无需清扫、无需心跳注册表；最坏浪费 = max_batch / rate 秒。
* 同步延迟不放大：计数器单调递增，重试 / 网络延迟 / 时钟偏移只会产生
  "空隙"（少打），结构上不可能产生"突发"（多打）。协调器不可达时
  fail-closed：宁可等待重试，绝不退化为本地限速自行放行。

生产部署用 :class:`RedisBackend`；:class:`InMemoryBackend` 语义与 Lua
脚本完全一致，仅供单进程开发 / 测试。
"""
from __future__ import annotations

import math
import random
import threading
import time
from collections import deque
from typing import NamedTuple

__all__ = [
    'GlobalRateLimiter', 'Lease', 'QuotaUnavailable',
    'RedisBackend', 'InMemoryBackend', 'RESERVE_LUA',
]


class QuotaUnavailable(Exception):
    """在调用方给定的预算内拿不到全局配额。

    fail-closed 语义：抛出此异常时调用方必须重试 / 排队（Celery 里用
    ``autoretry_for``），**禁止**退化为本地限速自行放行 —— 那正是
    "小配额被放大成大流量"的来源。
    """


class Lease(NamedTuple):
    """一次租约：count 个许可，首个在 start_us（协调器时钟）生效。"""

    start_us: int     # 本批第一个许可的生效时刻（协调器时钟，微秒）
    interval_us: int  # 相邻许可间隔（微秒）= 1e6 / rate
    count: int        # 本批许可数
    now_us: int       # 协调器当前时刻（微秒），用于客户端锚定本地时钟


# 原子租约脚本。全程整数微秒运算：Redis Lua 会把浮点返回值截断成整数，
# 微秒整数既保精度又避开这个坑。时钟一律取 Redis 服务器的 TIME，
# 不信任任何客户端时钟。
RESERVE_LUA = """
local t          = redis.call('TIME')
local now_us     = t[1] * 1000000 + t[2]
local rate       = tonumber(ARGV[1])
local want       = tonumber(ARGV[2])
local max_batch  = tonumber(ARGV[3])
local ttl_ms     = tonumber(ARGV[4])

local k          = math.min(want, max_batch)
local interval   = math.max(1, math.floor(1000000 / rate))

local next_us    = tonumber(redis.call('GET', KEYS[1])) or now_us
if next_us < now_us then
    next_us = now_us          -- 空闲期不攒额度：重新从 now 起排，杜绝补发突发
end
local start_us   = next_us
redis.call('SET', KEYS[1], start_us + k * interval, 'PX', ttl_ms)
return {start_us, interval, k, now_us}
"""


class RedisBackend:
    """生产后端：所有节点共用同一个 Redis key。

    每次租约一次往返（EVALSHA）。协调器 QPS ≈ 节点数 / lease_seconds，
    常规规模下对 Redis 可忽略。
    """

    def __init__(self, client, key='globalquota', ttl_ms=60_000):
        self._reserve = client.register_script(RESERVE_LUA)
        self.key = key
        self.ttl_ms = ttl_ms

    def reserve(self, rate, want, max_batch):
        start, interval, k, now = self._reserve(
            keys=[self.key],
            args=[rate, want, max_batch, self.ttl_ms],
        )
        return Lease(int(start), int(interval), int(k), int(now))


class InMemoryBackend:
    """与 RESERVE_LUA 完全相同的语义，但只在单进程内有效。

    用于本地开发和测试；多节点部署必须换成 RedisBackend，
    否则就退回了"每个进程一份配额"的老问题。
    """

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._lock = threading.Lock()
        self._next_us = None

    def reserve(self, rate, want, max_batch):
        with self._lock:
            now_us = int(self._clock() * 1_000_000)
            k = min(want, max_batch)
            interval = max(1, int(1_000_000 / rate))
            next_us = self._next_us if self._next_us is not None else now_us
            if next_us < now_us:
                next_us = now_us
            self._next_us = next_us + k * interval
            return Lease(next_us, interval, k, now_us)


class GlobalRateLimiter:
    """全局限速器。每个节点一个实例，底层共享同一个 backend key。

    参数：
        rate:          全集群合计速率（许可 / 秒）。
        lease_seconds: 单次租约覆盖的时长。租约批量 = rate * lease_seconds
                       （受 max_batch 封顶）。越小越平滑、节点掉线浪费越小，
                       代价是协调器 QPS = 节点数 / lease_seconds 略高。
        max_batch:     单次租约的硬上限（许可数）。即使客户端出 bug 或
                       被重试风暴冲击，单次最多也只能预约这么多个未来槽位，
                       而槽位本身严格等距 —— 突发在结构上不可能发生。

    线程安全；在 gevent/eventlet 下请先 monkey.patch_all()（常规做法），
    让锁和 sleep 变成协作式的。
    """

    def __init__(self, backend, *, rate, lease_seconds=1.0, max_batch=None):
        if rate <= 0:
            raise ValueError('rate must be > 0')
        self.backend = backend
        self.rate = float(rate)
        batch = max(1, math.ceil(self.rate * lease_seconds))
        if max_batch is not None:
            batch = min(batch, int(max_batch))
        self.batch = batch
        self._lock = threading.Lock()
        self._permits = deque()  # 本地待生效许可的生效时刻（单调时钟，秒）

    # -- 内部 ---------------------------------------------------------

    def _refill_locked(self):
        lease = self.backend.reserve(self.rate, self.batch, self.batch)
        anchor = time.monotonic()
        # 用协调器返回的 now 锚定本地单调时钟。网络往返只会让我们
        # "晚到"（start - now 偏大），方向上是保守安全的，绝不会提前开火。
        t0 = anchor + max(0.0, (lease.start_us - lease.now_us) / 1e6)
        step = lease.interval_us / 1e6
        self._permits.extend(t0 + i * step for i in range(lease.count))

    # -- 对外 ---------------------------------------------------------

    def acquire(self, timeout=None):
        """阻塞直到获得一个全局许可、并等到其生效时刻。

        timeout 内拿不到（协调器不可达，或全局队列已排到太远）则抛
        :class:`QuotaUnavailable` —— fail-closed，调用方必须重试，
        不得自行放行。
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        pause = 0.05
        while True:
            with self._lock:
                retry_in = None
                if not self._permits:
                    try:
                        self._refill_locked()
                    except Exception as exc:  # 协调器不可达：只重试，不放行
                        if deadline is not None and \
                                time.monotonic() + pause >= deadline:
                            raise QuotaUnavailable(
                                'quota coordinator unreachable') from exc
                        retry_in = pause * (0.5 + random.random())  # 退避 + 抖动
                        pause = min(pause * 2.0, 1.0)
                    else:
                        pause = 0.05
                if retry_in is None:
                    fire = self._permits[0]
                    if deadline is not None and fire > deadline:
                        # 许可排得太远，调用方等不起：不弹出，留给下次。
                        raise QuotaUnavailable(
                            f'next permit is {fire - time.monotonic():.2f}s '
                            f'away, beyond acquire timeout')
                    self._permits.popleft()
                    break
            time.sleep(retry_in)
        delay = fire - time.monotonic()
        if delay > 0:
            time.sleep(delay)
