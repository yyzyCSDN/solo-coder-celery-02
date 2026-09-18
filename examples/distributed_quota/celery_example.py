"""Celery 接入示例：把全局限速器挂到任务上。

关键约定：配额获取放在任务体（run）内的第一行，而不是 ``before_start``。
``before_start`` 在 ``celery/app/trace.py`` 里、autoretry 包装之外被调用，
在那里抛 QuotaUnavailable 不会触发自动重试；而 autoretry 只包装 ``run``
（见 ``celery/app/autoretry.py``），所以要在 run 之内获取配额。

运行方式（先起 Redis）：
    celery -A celery_example worker -l info -c 4   # 任意起多少个节点
    python -c "from celery_example import call_external_api; \
               [call_external_api.delay(i) for i in range(200)]"
无论起多少个 worker、每个 worker 多少个进程，对外部接口的调用总量
都稳定在 10 次/秒，且均匀放行。
"""
import redis
from celery import Celery, Task

from globalquota import GlobalRateLimiter, QuotaUnavailable, RedisBackend

app = Celery('proj', broker='redis://localhost:6379/0')

# 全局限速器：所有节点、所有进程共享 Redis 里的同一个 key。
# rate=10/s 是全集群合计；lease_seconds=1.0 表示每次租 1 秒的额度，
# 因此单节点突发 ≤ 10 次，节点掉线的最坏浪费 ≤ 1 秒的额度。
ext_api_limiter = GlobalRateLimiter(
    RedisBackend(redis.Redis(host='localhost', port=6379, db=1),
                 key='quota:ext-api'),
    rate=10.0,
    lease_seconds=1.0,
)


class QuotaLimitedTask(Task):
    """抽象基类：子类通过 limiter 指定自己的全局限速器。

    quota_wait 是在 worker 进程里阻塞等待配额的上限。等不到就抛
    QuotaUnavailable，由 autoretry 把任务退回队列稍后重试 ——
    fail-closed，绝不退化为本地限速。

    注意：prefork 池里阻塞会占住一个子进程，这本身就是想要的背压；
    若等待经常偏长，调小 quota_wait 让任务更快回到队列，或减少
    worker 并发，而不是放大配额。
    """

    abstract = True
    limiter: GlobalRateLimiter = None
    quota_wait = 2.0
    autoretry_for = (QuotaUnavailable,)
    retry_backoff = True
    retry_jitter = True
    retry_kwargs = {'max_retries': None}

    def acquire_quota(self):
        """在 run 的第一行调用。"""
        self.limiter.acquire(timeout=self.quota_wait)


class ExtApiTask(QuotaLimitedTask):
    abstract = True
    limiter = ext_api_limiter


@app.task(base=ExtApiTask, bind=True)
def call_external_api(self, payload):
    self.acquire_quota()          # 必须是任务体第一行
    # …… 从这里往下才真正调用外部接口 ……
    return {'payload': payload}


# 多个任务类型共用同一个外部接口的配额？让它们引用同一个 limiter
# （或各自建 limiter 但指向同一个 Redis key）即可，额度是同一份。
