"""无需 Redis 的仿真测试：InMemoryBackend 与 RESERVE_LUA 语义一致，
用多线程模拟多节点，验证全局限速器的四个关键性质：

1. 总量跨节点共享，不随节点数放大；
2. 任意滑动窗口内无突发（匀速放行）；
3. 节点掉线后，它占的额度自动回到池子里被幸存者使用；
4. 协调器故障期间 fail-closed，恢复后不补发、不放大；
5. 租约接口在并发重试风暴下，许可时刻表依旧严格等距。

运行：python -m pytest examples/distributed_quota/test_globalquota.py -v
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from globalquota import GlobalRateLimiter, InMemoryBackend, QuotaUnavailable

RATE = 40.0                 # 许可/秒（全集群合计）
INTERVAL_US = int(1e6 / RATE)
LEASE_SECONDS = 0.5
BATCH = 20                  # = ceil(RATE * LEASE_SECONDS)


class FlakyBackend(InMemoryBackend):
    """可注入网络延迟和故障窗口的后端（故障窗口为相对构造时刻的偏移秒）。"""

    def __init__(self, *args, latency=0.0, outage_offsets=(), **kwargs):
        super().__init__(*args, **kwargs)
        self.latency = latency
        t0 = time.monotonic()
        self.outages = [(t0 + s, t0 + e) for s, e in outage_offsets]

    def reserve(self, rate, want, max_batch):
        if self.latency:
            time.sleep(self.latency)
        now = time.monotonic()
        if any(s <= now < e for s, e in self.outages):
            raise ConnectionError('simulated coordinator outage')
        return super().reserve(rate, want, max_batch)


def run_nodes(backend, n_nodes, duration, kill=None, lease_seconds=LEASE_SECONDS):
    """起 n_nodes 个"节点"（线程）跑 duration 秒，返回 [(node_id, 相对时刻)]。"""
    kill = kill or {}
    calls = []
    lock = threading.Lock()
    t0 = time.monotonic()

    def is_dead(n):
        return n in kill and time.monotonic() - t0 >= kill[n]

    def node(n):
        limiter = GlobalRateLimiter(backend, rate=RATE,
                                    lease_seconds=lease_seconds)
        while time.monotonic() - t0 < duration:
            if is_dead(n):
                return            # 模拟节点掉线：直接消失，不做任何清理
            try:
                limiter.acquire(timeout=0.3)
            except QuotaUnavailable:
                time.sleep(0.05)  # 拿不到就退避重试（对应 Celery 的 autoretry）
                continue
            if is_dead(n):
                return            # 掉线节点即使持有许可也不会再开火
            with lock:
                calls.append((n, time.monotonic() - t0))

    threads = [threading.Thread(target=node, args=(i,), daemon=True)
               for i in range(n_nodes)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return sorted(calls, key=lambda c: c[1])


def windowed_max(calls, window):
    """任意滑动窗口内的最大调用数。"""
    ts = [t for _, t in calls]
    best = j = 0
    for i, t in enumerate(ts):
        while ts[j] <= t - window:
            j += 1
        best = max(best, i - j + 1)
    return best


def test_total_rate_is_shared_across_nodes():
    """8 个节点合计不得超过 RATE —— 若按 Celery 每进程限速，这里会是 8 倍。"""
    calls = run_nodes(FlakyBackend(), n_nodes=8, duration=4.0)
    steady = [c for c in calls if 0.5 < c[1] < 3.5]
    assert len(steady) <= RATE * 3.0 + 8 * BATCH      # 上限：绝不放大
    assert len(steady) >= RATE * 3.0 * 0.8            # 下限：吞吐基本打满


def test_no_burst_in_any_window():
    """匀速：任意 1s 窗口不超过 RATE（留调度抖动余量），0.25s 窗口同理。"""
    calls = run_nodes(FlakyBackend(), n_nodes=8, duration=4.0)
    assert windowed_max(calls, 1.0) <= RATE + 16
    assert windowed_max(calls, 0.25) <= RATE * 0.25 + 8


def test_dead_node_quota_is_reclaimed():
    """6 个节点杀掉 2 个：幸存者应吸收全部额度，且全程不超总量。"""
    calls = run_nodes(FlakyBackend(), n_nodes=6, duration=5.0,
                      kill={1: 2.0, 4: 2.0}, lease_seconds=0.2)
    # 掉线节点死后不再产生任何调用
    assert not [t for n, t in calls
                if n in (1, 4) and t > 2.0 + 0.45]
    # 幸存者把额度接了回来：后段总量仍接近 RATE，而不是 4/6 * RATE
    late = [t for _, t in calls if 3.0 <= t < 4.5]
    assert len(late) >= RATE * 1.5 * 0.7
    # 且任何窗口不超总量
    assert windowed_max(calls, 1.0) <= RATE + 16


def test_outage_fail_closed_and_no_catchup_burst():
    """协调器故障 1s：期间最多把已租出的许可打完；恢复后不补发积压。"""
    backend = FlakyBackend(outage_offsets=[(2.0, 3.0)])
    calls = run_nodes(backend, n_nodes=6, duration=5.0)
    during = [t for _, t in calls if 2.05 <= t <= 2.95]
    assert len(during) <= 6 * BATCH                 # 只有故障前已租出的许可
    after = [(n, t) for n, t in calls if t >= 3.0]
    # 故障期的额度是"作废"而不是"补发"：恢复后任意 0.5s 不超正常速率
    assert windowed_max(after, 0.5) <= RATE * 0.5 + 12


def test_sync_latency_does_not_amplify():
    """每次租约带 50ms 网络延迟：总量与匀速性不受影响。"""
    calls = run_nodes(FlakyBackend(latency=0.05), n_nodes=8, duration=4.0)
    assert windowed_max(calls, 1.0) <= RATE + 16
    steady = [c for c in calls if 0.5 < c[1] < 3.5]
    assert len(steady) >= RATE * 3.0 * 0.8


def test_concurrent_reserves_never_exceed_rate():
    """重试风暴下直接打租约接口：许可时刻表依旧严格等距，结构上无突发。"""
    backend = InMemoryBackend()
    grants = []
    lock = threading.Lock()

    def hammer():
        local = []
        for _ in range(200):
            lease = backend.reserve(RATE, want=10, max_batch=10)
            local.extend(lease.start_us + i * lease.interval_us
                         for i in range(lease.count))
        with lock:
            grants.extend(local)

    threads = [threading.Thread(target=hammer) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    grants.sort()
    gaps = [b - a for a, b in zip(grants, grants[1:])]
    assert min(gaps) >= INTERVAL_US * 0.99
