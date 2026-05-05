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
}


POST_SYNC_FIXES = {
    "tests_sync/test_cluster_operations.py": {
        "import redis.asyncio as aioredis": "import redis as aioredis",
        "conn.aclose()": "conn.close()",
        "asyncio.gather(*tasks)": "tasks",
    }
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
