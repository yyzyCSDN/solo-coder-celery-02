"""Distributed (cross-process) rate limiting utilities."""

from collections import deque

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None

__all__ = ('RedisTokenBucket', 'redis_client_from_url')

logger = get_logger(__name__)

#: Base class of redis-py exceptions, or an empty tuple when redis-py is
#: not installed (in which case no bucket can be created anyway, so the
#: ``except`` clauses below are never reached).
RedisError = redis.exceptions.RedisError if redis is not None else ()

#: Lua script implementing the token bucket on the Redis server.
#:
#: KEYS[1] is the bucket key (a hash with fields ``tokens`` and ``ts``).
#: ARGV is ``{fill_rate, capacity, requested, consume, ttl_ms}``.
#:
#: Returns ``{consumed, wait}`` where ``consumed`` is 1 if ``requested``
#: tokens were taken out of the bucket (only happens when ``consume``
#: is 1), and ``wait`` is the time in seconds until ``requested`` tokens
#: will be available (0 when they already are).
#:
#: The whole check-refill-consume cycle is a single atomic server-side
#: operation driven by the Redis server clock, so any number of workers
#: can share one bucket without races or clock skew between nodes, and
#: the token count can never exceed ``capacity`` no matter how long a
#: worker (or the bucket itself) was idle.
ACQUIRE_TOKENS_SCRIPT = """
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local consume = ARGV[4] == '1'
local ttl_ms = tonumber(ARGV[5])

local now = redis.call('TIME')
local nowf = now[1] + now[2] / 1000000

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
    tokens = capacity
    ts = nowf
end

local elapsed = nowf - ts
if elapsed > 0 then
    tokens = math.min(capacity, tokens + elapsed * rate)
end

local consumed = 0
local wait = 0.0
if tokens >= requested then
    if consume then
        tokens = tokens - requested
        consumed = 1
    end
else
    wait = (requested - tokens) / rate
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(nowf))
redis.call('PEXPIRE', KEYS[1], ttl_ms)
return {consumed, tostring(wait)}
"""


class RedisTokenBucket:
    """Token bucket shared between all workers using Redis.

    Drop-in replacement for :class:`kombu.utils.limits.TokenBucket`
    (it implements the interface used by the worker consumer), but the
    bucket state lives in Redis so the rate limit is enforced *globally*
    -- across all worker processes and nodes -- instead of per process.

    Every token acquisition is a single atomic operation on the Redis
    server, using the server clock, which gives the bucket the
    following properties:

    * The limit cannot be exceeded by adding workers: every worker
      draws tokens from the same bucket.
    * Tokens are refilled at ``fill_rate`` per second and capped at
      ``capacity`` (1 by default), so tasks are released evenly and no
      burst can accumulate while a worker is paused, restarting or
      unable to reach Redis.  A worker coming back after any delay
      finds at most ``capacity`` tokens, never a stockpiled backlog.
    * Workers never hold quota: a token is only spent at the moment a
      task is released.  If a worker goes offline there is nothing to
      reclaim -- the remaining workers immediately share the full rate.
    * The bucket fails *closed*: if Redis cannot be reached no tokens
      are consumed and tasks stay queued (to be retried shortly)
      instead of being released without limit.

    The bucket key expires automatically after a period of inactivity
    (at least twice the time needed to refill a drained bucket), so
    removing a task type or changing its rate does not leave stale
    state behind.
    """

    #: Redis key prefix used for all rate limit buckets.
    KEY_PREFIX = 'celery:ratelimit'

    #: Time in seconds to wait before retrying when Redis cannot
    #: be reached.
    RETRY_INTERVAL = 1.0

    #: Minimum key TTL, in milliseconds.
    MIN_KEY_TTL_MS = 60 * 1000

    def __init__(self, fill_rate, capacity=1, client=None, key=None):
        if client is None:
            raise ImproperlyConfigured(
                'RedisTokenBucket requires a Redis client')
        self.fill_rate = float(fill_rate)
        self.capacity = float(capacity)
        self.client = client
        self.key = f'{self.KEY_PREFIX}:{key}'
        #: Pending (request, tokens) pairs.  These are local to the
        #: process, exactly like kombu's TokenBucket: only the token
        #: state is shared between workers.
        self.contents = deque()
        self._acquire_script = client.register_script(ACQUIRE_TOKENS_SCRIPT)
        # Keep the key alive for at least twice the time needed to fully
        # refill a drained bucket, so the state cannot expire while
        # workers are still waiting for tokens.
        self._key_ttl_ms = max(
            self.MIN_KEY_TTL_MS,
            int(2000 * self.capacity / self.fill_rate),
        )

    def can_consume(self, tokens=1):
        """Check if tokens can be consumed from the bucket.

        Returns:
            bool: true if the number of tokens could be consumed, in
                which case they are consumed atomically.  Returns false
                if not enough tokens are available, or if Redis cannot
                be reached (fail-closed).
        """
        try:
            consumed, _ = self._acquire(tokens, consume=True)
        except RedisError as exc:
            logger.warning(
                'Cannot acquire rate limit token for %r: %r. '
                'Delaying task.', self.key, exc)
            return False
        return consumed

    def expected_time(self, tokens=1):
        """Return estimated time until tokens are available.

        Does not consume any tokens.

        Returns:
            float: the time in seconds, or :attr:`RETRY_INTERVAL` if
                Redis cannot be reached.
        """
        try:
            _, wait = self._acquire(tokens, consume=False)
        except RedisError as exc:
            logger.warning(
                'Cannot query rate limit bucket %r: %r. '
                'Retrying in %s seconds.', self.key, exc, self.RETRY_INTERVAL)
            return self.RETRY_INTERVAL
        return wait

    def _acquire(self, tokens, consume):
        consumed, wait = self._acquire_script(
            keys=[self.key],
            args=[self.fill_rate, self.capacity, tokens,
                  1 if consume else 0, self._key_ttl_ms],
            client=self.client,
        )
        return bool(int(consumed)), float(wait)

    def add(self, item):
        self.contents.append(item)

    def pop(self):
        return self.contents.popleft()

    def clear_pending(self):
        self.contents.clear()


def redis_client_from_url(url, **kwargs):
    """Create a Redis client from a URL.

    The client connects lazily on first use.  Conservative socket
    timeouts are set by default so a hanging Redis server cannot block
    the worker indefinitely; they can be overridden with keyword
    arguments or URL query parameters.

    Raises:
        ImproperlyConfigured: if the redis library is not installed.
    """
    if redis is None:
        raise ImproperlyConfigured(
            'Distributed rate limits need the redis library.\n'
            'You can install it with: pip install "celery[redis]"')
    kwargs.setdefault('socket_connect_timeout', 1.0)
    kwargs.setdefault('socket_timeout', 2.0)
    return redis.Redis.from_url(url, **kwargs)
