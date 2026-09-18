# 跨节点共享配额限速器

解决的核心问题：Celery 自带的 `rate_limit` 是在**每个 worker 进程**内各建一个
本地令牌桶（`celery/worker/consumer/consumer.py` 的 `bucket_for_task` →
kombu `TokenBucket`），节点数 × 进程数会把对外部接口的实际调用量放大。
本方案把配额收归协调器（Redis），全集群共享同一条发放时刻表。

## 设计：全局虚拟时钟租约（batched GCRA）

配额不是"令牌库存"，而是**时间**。Redis 里只存一个单调时间戳 `next_us`
（下一个可用槽位的生效时刻）。一条 Lua 脚本原子地完成租约：

```
next_us = max(next_us, now)          -- 空闲期不攒额度
start   = next_us                    -- 本批第一个许可的生效时刻
next_us = start + k * (1/rate)       -- 把时刻表往后推 k 格
return (start, interval, k, now)
```

节点拿到租约后，只在本地按时刻表"开火"（sleep 到每个许可的生效时刻）。
时钟一律取 Redis 服务器的 `TIME`，不信任任何客户端时钟。

## 四个关键性质

| 需求 | 机制 |
|---|---|
| 跨节点共享配额 | 所有节点共用同一个 key，总量 = `rate`，与节点数、进程数无关 |
| 按整体额度匀速放行 | 许可生效时刻严格等距（1/rate），跨节点也等距；空闲期不攒额度（`max(next_us, now)`），不存在"补发突发" |
| 节点掉线额度归还 | 额度是时间不是库存：掉线节点预约的未来槽位自然过期作废，无需心跳、无需清扫。最坏浪费 = 掉线节点数 × `max_batch` / rate 秒 |
| 同步延迟不放大 | 计数器单调递增，重试/延迟/时钟偏移只会产生**空隙**（少打），结构上不可能产生**突发**（多打）；协调器不可达时 fail-closed，只重试不放行 |

对比常见的"令牌桶 + 窗口批取"方案：那种方案要正确，需要额外做窗口幂等
（防重试双花）、活跃节点注册表（防单节点掏空）、本地 pacing（防窗口内
扎堆），每一步都是新的故障面。虚拟时钟把这些性质变成了结构性的。

## 故障行为速查

| 场景 | 行为 | 方向 |
|---|---|---|
| 节点掉线 | 它预约的未来槽位作废，幸存者自动吸收全部额度 | 短暂少打（≤ 掉线节点数 × batch/rate 秒） |
| Redis 不可达 | 节点打完手头已租许可后停摆，退避重试 | fail-closed，零放行 |
| Redis 恢复 | 从 `now` 重新排时刻表，故障期的额度**作废不补发** | 无补发突发 |
| 客户端重试风暴 | 重复租约只会把槽位排得更靠后 | 产生空隙，不产生突发 |
| Redis 主从切换丢 key | 已租出的许可（≤ 节点数 × batch）与新时刻表短暂并存 | 瞬时 ≤ 2×rate，把 batch 调小即可收敛 |

## 参数怎么调

- `rate`：全集群合计速率，直接按外部接口的预算设。
- `lease_seconds`（默认 1s）：单次租约覆盖的时长。决定三件事——
  单节点突发上限（= rate × lease_seconds）、节点掉线的最坏浪费、
  协调器 QPS（= 节点数 / lease_seconds）。接口越敏感就调越小。
- `max_batch`：单次租约硬上限，防客户端 bug 把时刻表推到遥远的未来。
- `quota_wait`（Celery 侧）：worker 里阻塞等配额的上限，等不到就把任务
  退回队列重试（`autoretry_for=(QuotaUnavailable,)`）。

## 文件

- `globalquota.py` — 限速器本体（`RedisBackend` 生产用，`InMemoryBackend`
  语义与 Lua 完全一致，供单进程开发/测试）
- `celery_example.py` — Celery 接入示例（配额获取放在任务体第一行，
  让 `autoretry_for` 能兜住 `QuotaUnavailable`）
- `test_globalquota.py` — 无需 Redis 的多节点仿真测试，
  覆盖上表全部故障场景

## 监控建议

- 盯 Redis key 的值（`next_us`）与当前时间的差：持续拉大说明配额长期
  供不应求（该扩容预算或削峰），而不是限速器坏了。
- 应用侧统计 `QuotaUnavailable` 次数：它就是"被全局限流挡住"的指标。
