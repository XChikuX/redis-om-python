import asyncio
import random

import pytest

from aredis_om import get_redis_connection
from aredis_om.model.model import model_registry

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


@pytest.fixture(autouse=True)
def cleanup_model_registry():
    existing_models = set(model_registry)

    yield

    # Models defined inside test functions (e.g. ``class Inner``) register
    # as ``__main__.<Name>`` rather than ``tests.*``.  They hold references
    # to the test's ``redis`` fixture, which is torn down afterwards.  If
    # we leave them in ``model_registry`` a later test that reuses the same
    # class name will look up the dead connection and silently find no
    # results, causing brittle order-dependent failures.
    for key in list(model_registry):
        if key not in existing_models:
            model_registry.pop(key, None)
        else:
            # Module-level test models (e.g. ``tests.test_migrator_alias._PersonV1``)
            # are also stripped out between tests so that a bare
            # ``Migrator(conn=redis).run()`` in an unrelated test does not pick
            # them up and try to manage their indexes against the test's
            # single-node Redis. The fixtures that need them re-register the
            # class via ``_isolate_registry`` before each test runs.
            str_key = str(key)
            if str_key.startswith("tests.test_migrator_alias.") or str_key.startswith(
                "tests.test_cluster_migrator_alias."
            ):
                model_registry.pop(key, None)
