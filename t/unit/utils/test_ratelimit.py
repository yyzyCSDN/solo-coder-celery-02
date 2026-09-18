import time
from unittest.mock import Mock

import pytest

redis = pytest.importorskip('redis')
fakeredis = pytest.importorskip('fakeredis')

from celery.exceptions import ImproperlyConfigured  # noqa: E402
from celery.utils import ratelimit  # noqa: E402
from celery.utils.ratelimit import (  # noqa: E402
    RedisTokenBucket, redis_client_from_url,
)


@pytest.fixture
def server():
    return fakeredis.FakeServer()


@pytest.fixture
def client(server):
    return fakeredis.FakeRedis(server=server)


@pytest.fixture
def bucket(client):
    return RedisTokenBucket(100, capacity=1, client=client, key='task.add')


class test_RedisTokenBucket:

    def test_starts_full(self, bucket):
        assert bucket.can_consume()

    def test_empty_after_consume(self, bucket):
        assert bucket.can_consume()
        assert not bucket.can_consume()

    def test_refills_at_fill_rate(self, bucket):
        assert bucket.can_consume()
        assert not bucket.can_consume()
        time.sleep(0.05)  # 5 tokens worth at 100/s, clamped to capacity
        assert bucket.can_consume()

    def test_tokens_do_not_accumulate_beyond_capacity(self, bucket):
        # a worker that was paused or offline for a long time must not
        # find a stockpiled backlog when it comes back.
        time.sleep(0.5)  # 50 tokens worth at 100/s, capacity is 1
        assert bucket.can_consume()
        assert not bucket.can_consume()
        assert 0 < bucket.expected_time() <= 0.01

    def test_shared_between_clients(self, server, client):
        other_client = fakeredis.FakeRedis(server=server)
        bucket_a = RedisTokenBucket(100, client=client, key='task.add')
        bucket_b = RedisTokenBucket(100, client=other_client, key='task.add')
        # a token spent by one worker is gone for all of them, so a
        # worker going offline has nothing to hand back: the rest
        # simply keep sharing the same bucket.
        assert bucket_a.can_consume()
        assert not bucket_b.can_consume()

    def test_separate_keys_have_separate_buckets(self, client):
        bucket_a = RedisTokenBucket(100, client=client, key='task.add')
        bucket_b = RedisTokenBucket(100, client=client, key='task.mul')
        assert bucket_a.can_consume()
        assert bucket_b.can_consume()

    def test_expected_time_zero_when_available(self, bucket):
        assert bucket.expected_time() == 0

    def test_expected_time_when_drained(self, bucket):
        assert bucket.can_consume()
        expected = bucket.expected_time()
        assert 0 < expected <= 0.01

    def test_expected_time_does_not_consume(self, bucket):
        assert bucket.can_consume()
        expected = bucket.expected_time()
        time.sleep(expected + 0.02)
        assert bucket.can_consume()

    def test_rate_is_enforced_over_time(self, client):
        bucket = RedisTokenBucket(100, capacity=1, client=client,
                                  key='task.add')
        consumed = 0
        start = time.monotonic()
        while time.monotonic() - start < 0.2:
            if bucket.can_consume():
                consumed += 1
            else:
                time.sleep(0.001)
        elapsed = time.monotonic() - start
        # never more than the refill plus the initial capacity,
        # allowing a little slack for timing jitter.
        assert consumed <= elapsed * 100 + 1 + 2
        assert consumed >= 5

    def test_key_expires(self, client, bucket):
        assert bucket.can_consume()
        ttl = client.pttl(bucket.key)
        assert 0 < ttl <= bucket._key_ttl_ms

    def test_key_ttl_covers_full_refill(self, client):
        slow = RedisTokenBucket(1 / 3600, capacity=1, client=client,
                                key='task.slow')
        # the key must live at least twice the time needed to refill
        # a drained bucket (1h at 1/h).
        assert slow._key_ttl_ms >= 2 * 3600 * 1000

    def test_fails_closed_when_redis_is_down(self):
        client = Mock(name='redis')
        client.register_script.return_value = Mock(
            side_effect=redis.exceptions.ConnectionError('boom'))
        bucket = RedisTokenBucket(100, client=client, key='task.add')
        assert not bucket.can_consume()
        assert bucket.expected_time() == bucket.RETRY_INTERVAL

    def test_requires_client(self):
        with pytest.raises(ImproperlyConfigured):
            RedisTokenBucket(100, client=None, key='task.add')

    def test_pending_queue_interface(self, bucket):
        bucket.add(('req1', 1))
        bucket.add(('req2', 1))
        assert bucket.pop() == ('req1', 1)
        bucket.contents.appendleft(('req0', 1))
        assert bucket.pop() == ('req0', 1)
        bucket.clear_pending()
        with pytest.raises(IndexError):
            bucket.pop()


class test_redis_client_from_url:

    def test_creates_client_with_timeouts(self):
        client = redis_client_from_url('redis://localhost:6379/0')
        pool_kwargs = client.connection_pool.connection_kwargs
        assert pool_kwargs['socket_connect_timeout'] == 1.0
        assert pool_kwargs['socket_timeout'] == 2.0

    def test_raises_without_redis_library(self, monkeypatch):
        monkeypatch.setattr(ratelimit, 'redis', None)
        with pytest.raises(ImproperlyConfigured):
            redis_client_from_url('redis://localhost:6379/0')
