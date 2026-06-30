import os
from pathlib import Path

import unasync

ADDITIONAL_REPLACEMENTS = {
    "aredis_om": "redis_om",
    "async_redis": "sync_redis",
    "redis.asyncio as aioredis": "redis as aioredis",
    ":tests.": ":tests_sync.",
    "pytest_asyncio": "pytest",
    "py_test_mark_asyncio": "py_test_mark_sync",
    "pytest.mark.asyncio(f)": "f",
    "pytest.mark.asyncio": "py_test_mark_sync",
    ".aclose()": ".close()",
    # NOTE: unasync strips `await` from any expression, so transforming
    # ``asyncio.sleep(`` here is undone when unasync removes the
    # ``await`` keyword and re-emits the call. The actual replacement
    # is done in ``POST_SYNC_FIXES`` below.
}


POST_SYNC_FIXES = {
    "tests_sync/test_cluster_operations.py": {
        "import redis.asyncio as aioredis": "import redis as aioredis",
        "conn.aclose()": "conn.close()",
        # In the generated sync mirror these call sites already contain eager
        # return values, not coroutines, so the async gather wrapper must be
        # removed.
        "asyncio.gather(*tasks)": "tasks",
    },
    # The RESP3 accommodation tests use async-only ``aclose()`` for cleanup.
    "tests_sync/test_protocol_negotiation.py": {
        "conn.aclose()": "conn.close()",
    },
    # hotkeys_snapshot uses asyncio.sleep for the wait; the sync mirror
    # needs time.sleep instead.
    "redis_om/hotkeys.py": {
        "import asyncio": "import time",
        "asyncio.sleep(": "time.sleep(",
    },
    # ``_wait_for_index`` polls FT.INFO using ``await asyncio.sleep(0.05)``.
    # In the sync mirror that becomes a bare ``asyncio.sleep(0.05)`` which
    # returns a coroutine and is never awaited. Swap the module import and
    # rewrite the call site.
    "redis_om/model/migrations/migrator.py": {
        "import asyncio": "import time\n",
        "asyncio.sleep(": "time.sleep(",
        # The async source uses asyncio.get_event_loop().time() to track
        # the deadline; without an event loop we fall back to time.monotonic().
        "asyncio.get_event_loop().time()": "time.monotonic()",
    },
    # Hotkeys async tests use import asyncio + await asyncio.sleep / create_task.
    # After unasync strips await, the sync mirror needs time.sleep and no task.
    "tests_sync/test_observability_hotkeys.py": {
        "import asyncio": "import time",
        "asyncio.sleep(": "time.sleep(",
        "task = asyncio.create_task(gen_load())": "gen_load()",
        "        task\n": "",
    },
    # The strawberry integration tests use ``asyncio.get_running_loop()``
    # inside ``_find_with_retry``. In the sync mirror there's no running
    # loop, so we swap the call for ``time.monotonic()`` and rely on the
    # ``asyncio.sleep`` -> ``time.sleep`` rewrite below.
    "tests_sync/test_strawberry_integration.py": {
        "import asyncio": "import time",
        "asyncio.sleep(": "time.sleep(",
        "loop = asyncio.get_running_loop()\n    deadline = loop.time() + timeout\n    last_results: list = []\n    while loop.time() < deadline:": "deadline = time.monotonic() + timeout\n    last_results: list = []\n    while time.monotonic() < deadline:",
    },
    # The alias migrator tests define a local ``_wait_for_index_sync`` helper
    # that uses ``asyncio.get_event_loop().time()``. The sync mirror has no
    # event loop, so rewrite it to ``time.monotonic()`` (same treatment the
    # migrator module itself gets above).
    "tests_sync/test_migrator_alias.py": {
        "import asyncio": "import time",
        "asyncio.get_event_loop().time()": "time.monotonic()",
        "asyncio.sleep(": "time.sleep(",
    },
    # py_test_mark_asyncio becomes py_test_mark_sync in the mirror; its
    # body ``return pytest.mark.asyncio(f)`` must become ``return f`` so
    # sync test functions stay non-asyncio (unasync does not rewrite the
    # decorator inside the function body).
    "tests_sync/conftest.py": {
        "return pytest.mark.asyncio(f)": "return f",
        # The async docstring says "Returns pytest.mark.asyncio(f)"; in the
        # sync mirror that's no longer accurate.
        '    """Mark a test as async. Returns pytest.mark.asyncio(f) for decorator use."""\n': '    """No-op marker for sync tests (mirrors py_test_mark_asyncio)."""\n',
    },
    # The RESP3 bytes-key regression tests intentionally construct
    # ``redis.asyncio.Redis`` directly so they exercise the bytes-keys code
    # path.  In the sync mirror the asyncio import must be replaced with the
    # sync redis module so the generated tests exercise the same wire shapes
    # without awaiting coroutines.
    "tests_sync/test_from_redis_resp3.py": {
        "from redis import asyncio as aioredis": "import redis as aioredis",
    },
}

# Deduplicate `import pytest` lines that unasync may produce when
# `pytest_asyncio` → `pytest` replacement overlaps with existing imports.
_DUPLICATE_IMPORT_PYTEST = "\nimport pytest\nimport pytest\n"
_DEDUPED_IMPORT_PYTEST = "\nimport pytest\n"


def _dedupe_import_pytest(content: str) -> str:
    """Remove consecutive duplicate `import pytest` lines from generated files."""
    while _DUPLICATE_IMPORT_PYTEST in content:
        content = content.replace(_DUPLICATE_IMPORT_PYTEST, _DEDUPED_IMPORT_PYTEST)
    return content


def _fix_asyncio_sleep(content: str) -> str:
    """Convert bare ``asyncio.sleep(...)`` to ``time.sleep(...)`` in sync mirrors.

    unasync strips ``await`` from any expression, so a source-side
    ``await asyncio.sleep(x)`` becomes a bare ``asyncio.sleep(x)`` in the
    sync mirror. ``asyncio.sleep`` returns a coroutine object in Python
    3.12+, which raises ``RuntimeWarning: coroutine 'sleep' was never
    awaited`` (and breaks when ``asyncio`` is no longer imported).
    """
    if "asyncio.sleep(" in content:
        content = content.replace("asyncio.sleep(", "time.sleep(")

    # Drop ``import asyncio`` once no ``asyncio.*`` call remains. We scan
    # every line so occurrences in docstrings/comments don't count.
    still_used = False
    for line in content.splitlines():
        stripped = line.lstrip()
        if (
            stripped.startswith("#")
            or stripped.startswith('"""')
            or stripped.startswith("'''")
        ):
            continue
        if "asyncio." in line:
            still_used = True
            break
    if not still_used and "import asyncio\n" in content:
        content = content.replace("import asyncio\n", "")
    return content


def apply_post_sync_fixes(repo_root: Path):
    for relative_path, replacements in POST_SYNC_FIXES.items():
        file_path = repo_root / relative_path
        if not file_path.exists():
            continue

        content = file_path.read_text()
        updated = content
        for old, new in replacements.items():
            updated = updated.replace(old, new)

        if updated != content:
            file_path.write_text(updated)

    # Global dedupe of duplicate `import pytest` and asyncio.sleep
    # normalisation across all generated sync files.
    for prefix in ("redis_om", "tests_sync"):
        target_dir = repo_root / prefix
        if not target_dir.exists():
            continue
        for file_path in target_dir.rglob("*.py"):
            content = file_path.read_text()
            updated = _dedupe_import_pytest(content)
            updated = _fix_asyncio_sleep(updated)
            if updated != content:
                file_path.write_text(updated)


def main():
    repo_root = Path(__file__).absolute().parent
    rules = [
        unasync.Rule(
            fromdir="/aredis_om/",
            todir="/redis_om/",
            additional_replacements=ADDITIONAL_REPLACEMENTS,
        ),
        unasync.Rule(
            fromdir="/tests/",
            todir="/tests_sync/",
            additional_replacements=ADDITIONAL_REPLACEMENTS,
        ),
    ]
    filepaths = []
    for root, _, filenames in os.walk(repo_root):
        for filename in filenames:
            if filename.rpartition(".")[-1] in (
                "py",
                "pyi",
            ):
                filepaths.append(os.path.join(root, filename))

    unasync.unasync_files(filepaths, rules)
    apply_post_sync_fixes(repo_root)


if __name__ == "__main__":
    main()
