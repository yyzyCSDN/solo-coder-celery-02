====================================
 ``celery.utils.ratelimit``
====================================

.. contents::
    :local:

Distributed rate limiting utilities.

When the :setting:`worker_distributed_rate_limit_backend` setting is
configured, the worker uses :class:`RedisTokenBucket` instead of the
local :class:`kombu.utils.limits.TokenBucket` so that task rate limits
are enforced globally, across all worker processes and nodes.

API Reference
=============

.. currentmodule:: celery.utils.ratelimit

.. automodule:: celery.utils.ratelimit

    .. autoclass:: RedisTokenBucket

        .. automethod:: can_consume
        .. automethod:: expected_time
        .. automethod:: add
        .. automethod:: pop
        .. automethod:: clear_pending

    .. autofunction:: redis_client_from_url
