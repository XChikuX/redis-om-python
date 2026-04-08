import asyncio
import random

import pytest

from aredis_om import get_redis_connection

from ._sync_redis import get_sync_redis_connection

TEST_PREFIX = "redis-om:testing"


def py_test_mark_asyncio(f):
    """Mark a test as async. Returns pytest.mark.asyncio(f) for decorator use."""
    return pytest.mark.asyncio(f)


# NOTE: We intentionally do NOT define a custom event_loop fixture.
# pytest-asyncio >= 0.21 handles event loop lifecycle automatically,
# and a custom fixture triggers "unclosed event loop" deprecation warnings.


@pytest.fixture
def redis():
    yield get_redis_connection()


def _delete_test_keys(prefix: str, conn):
    keys = []
    for key in conn.scan_iter(f"{prefix}:*"):
        keys.append(key)
    if keys:
        conn.delete(*keys)


@pytest.fixture
def key_prefix(request, redis):
    key_prefix = f"{TEST_PREFIX}:{random.random()}"
    yield key_prefix


@pytest.fixture(scope="session", autouse=True)
def cleanup_keys(request):
    # Increment for every pytest-xdist worker
    conn = get_sync_redis_connection()
    once_key = f"{TEST_PREFIX}:cleanup_keys"
    conn.incr(once_key)

    yield

    # Delete keys only once
    if conn.decr(once_key) == 0:
        _delete_test_keys(TEST_PREFIX, conn)
