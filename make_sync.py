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
    # unasync strips `await` from asyncio.sleep as well, leaving a bare
    # call that returns a coroutine (mypy flags it, and runtime would
    # crash on NameError since `asyncio` is never imported in the
    # generated sync mirror). Rewrite to time.sleep explicitly.
    "asyncio.sleep(": "time.sleep(",
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
    # Hotkeys async tests use import asyncio + await asyncio.sleep / create_task.
    # After unasync strips await, the sync mirror needs time.sleep and no task.
    "tests_sync/test_observability_hotkeys.py": {
        "import asyncio": "import time",
        "asyncio.sleep(": "time.sleep(",
        "task = asyncio.create_task(gen_load())": "gen_load()",
        "        task\n": "",
    },
}


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
