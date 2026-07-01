import os
import re
from pathlib import Path
from typing import List

import unasync

# Modules that are intentionally async-only and must not be mirrored into
# the generated ``redis_om`` sync package.  Each entry is a path relative
# to the repo root using OS-native separators.
#
# ``integrations/fastapi_redis_sdk.py`` bridges with fastapi-redis-sdk,
# which manages ``redis.asyncio`` pools exclusively — there is no sync
# counterpart to generate.
_ASYNC_ONLY_DIRS = (os.path.join("aredis_om", "integrations"),)

# Tests that exercise async-only modules.  Listed explicitly because the
# unasync walker cannot tell from a directory alone which tests reference
# an async-only module — these are mirrored by file basename.
_ASYNC_ONLY_TEST_BASENAMES = frozenset(
    {
        # Bridge tests — would ``from redis_om.integrations import ...``
        # and fail to import because the sync mirror does not exist.
        "test_fastapi_integration.py",
    }
)

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
    # Fix Redis imports and remove async gather wrappers in cluster tests.
    "tests_sync/test_cluster_operations.py": {
        "import redis.asyncio as aioredis": "import redis as aioredis",
        "conn.aclose()": "conn.close()",
        "asyncio.gather(*tasks)": "tasks",
    },
    # RESP3 tests require sync close() instead of async aclose().
    "tests_sync/test_protocol_negotiation.py": {
        "conn.aclose()": "conn.close()",
    },
    # Convert asyncio sleep to time.sleep for hotkey snapshots.
    "redis_om/hotkeys.py": {
        "import asyncio": "import time",
        "asyncio.sleep(": "time.sleep(",
    },
    # Convert polling loop from asyncio.sleep to time.monotonic/sleep.
    "redis_om/model/migrations/migrator.py": {
        "import asyncio": "import time\n",
        "asyncio.sleep(": "time.sleep(",
        "asyncio.get_event_loop().time()": "time.monotonic()",
    },
    # Convert async observability tests (sleep and task creation) to sync.
    "tests_sync/test_observability_hotkeys.py": {
        "import asyncio": "import time",
        "asyncio.sleep(": "time.sleep(",
        "task = asyncio.create_task(gen_load())": "gen_load()",
        "        task\n": "",
    },
    # Convert loop-based retry logic to use time.monotonic/sleep.
    "tests_sync/test_strawberry_integration.py": {
        "import asyncio": "import time",
        "asyncio.sleep(": "time.sleep(",
        "loop = asyncio.get_running_loop()\n    deadline = loop.time() + timeout\n    last_results: list = []\n    while loop.time() < deadline:": "deadline = time.monotonic() + timeout\n    last_results: list = []\n    while time.monotonic() < deadline:",
    },
    # Replace asyncio loop time with monotonic in alias migrator helpers.
    "tests_sync/test_migrator_alias.py": {
        "import asyncio": "import time",
        "asyncio.get_event_loop().time()": "time.monotonic()",
        "asyncio.sleep(": "time.sleep(",
    },
    # Strip background migration tasks and convert loop time to monotonic.
    "tests_sync/test_cluster_migrator_alias.py": {
        "import asyncio": "import time",
        "asyncio.get_event_loop().time()": "time.monotonic()",
        "asyncio.sleep(": "time.sleep(",
        "import redis.asyncio as aioredis": "import redis as aioredis",
        "        migration_task = asyncio.create_task(\n            Migrator(conn=redis, allow_forward_swap=True).run()\n        )": "        Migrator(conn=redis, allow_forward_swap=True).run()",
        "        migration_task\n": "",
    },
    # Update docstring to reflect the sync nature of the marker.
    "tests_sync/conftest.py": {
        '    """Mark a test as async. Returns pytest.mark.asyncio(f) for decorator use."""\n': '    """No-op marker for sync tests (mirrors py_test_mark_asyncio)."""\n',
    },
    # Replace asyncio redis with sync redis to test RESP3 wire shapes.
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


def _fix_pytest_mark_asyncio_body(content: str) -> str:
    """
    Remove `pytest.mark.asyncio` from sync test bodies and decorators.

    Since unasync converts async tests to sync tests, any remaining
    `pytest.mark.asyncio` markers will cause pytest-asyncio to raise errors.

    This function:
    - Strips `return pytest.mark.asyncio(f)` from function bodies.
    - Removes `pytest.mark.asyncio` from `pytestmark` lists and decorators.
    - Ensures sync mirrors are valid without requiring manual `POST_SYNC_FIXES`.
    """
    content = content.replace("return pytest.mark.asyncio(f)", "return f")
    # Drop individual ``pytest.mark.asyncio`` decorator lines above test
    # functions. These can appear as a single-line decorator or as one
    # entry inside a ``pytestmark = [...]`` list.
    content = re.sub(
        r"^@pytest\.mark\.asyncio\s*$\n",
        "",
        content,
        flags=re.MULTILINE,
    )
    # Drop ``    pytest.mark.asyncio,`` (with trailing comma) from inside
    # multi-line ``pytestmark`` lists. Other entries in the same list are
    # preserved.
    content = re.sub(
        r"^[ \t]*pytest\.mark\.asyncio,?[ \t]*$\n",
        "",
        content,
        flags=re.MULTILINE,
    )
    return content


def _fix_asyncio_sleep(content: str) -> str:
    """
    Transform asyncio calls into synchronous equivalents for sync mirrors.

    - Converts `asyncio.sleep(x)` to `time.sleep(x)`.
    - Rewrites `asyncio.gather(*expr)` to `[None for _ in expr]` to ensure
      iterables are executed in sync mode.
    - Removes `import asyncio` and any associated `try/except` guards if
      no `asyncio` references remain.
    """
    if "asyncio.sleep(" in content:
        content = content.replace("asyncio.sleep(", "time.sleep(")

    if "asyncio.gather(" in content:
        # Uses a balanced-paren scan to correctly identify the
        # end of `EXPR` where regex would fail on nested parentheses.
        prefix = "asyncio.gather("
        rebuilt: List[str] = []
        cursor = 0
        while True:
            idx = content.find(prefix, cursor)
            if idx < 0:
                rebuilt.append(content[cursor:])
                break
            rebuilt.append(content[cursor:idx])
            depth = 1
            i = idx + len(prefix)
            while i < len(content) and depth > 0:
                ch = content[i]
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                i += 1
            inner = content[idx + len(prefix) : i - 1].lstrip()
            if inner.startswith("*"):
                inner = inner[1:].lstrip()
            # If the unpacking target is already a single parenthesised
            # expression (typical for ``*(gen_expr)``), reuse those parens
            # rather than wrapping in another set. Bare identifiers and
            # complex expressions get wrapped for safety.
            stripped = inner.strip()
            if (
                stripped.startswith("(")
                and stripped.endswith(")")
                and _is_balanced_paren_group(stripped)
            ):
                rebuilt.append(f"[None for _ in {stripped}]")
            else:
                rebuilt.append(f"[None for _ in ({inner})]")
            cursor = i
        content = "".join(rebuilt)

    # If a ``try: import asyncio / except ImportError: ...`` skip-guard is
    # present, remove it before stripping the import — otherwise the
    # ``try:`` ends up with an empty body.
    content = _strip_try_import_asyncio(content)

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


def _is_balanced_paren_group(text: str) -> bool:
    """Return True if ``text`` is a single balanced parenthesised group.

    ``text`` is expected to start with ``(`` and end with ``)``. We verify
    that the opening paren is matched by the closing one and that no
    unmatched parens appear between them. Used to detect whether an
    ``asyncio.gather(*X)`` argument is already wrapped in a redundant
    outer paren that we can reuse.
    """
    if not (text.startswith("(") and text.endswith(")")):
        return False
    depth = 0
    for i, ch in enumerate(text):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and i != len(text) - 1:
                return False
    return depth == 0


def _strip_try_import_asyncio(content: str) -> str:
    """Remove ``try: \n import asyncio \n except ImportError: <body>`` blocks.

    The ``try: import asyncio / except ImportError:`` idiom guards against
    a missing ``asyncio`` module — but asyncio is part of the stdlib on
    every Python version this project supports, so the guard is dead code
    in the sync mirror.  Removing the whole block (not just the import
    line) avoids leaving an empty ``try:`` body.
    """
    well_formed = re.compile(
        r"^(?P<indent>[ \t]*)try:\s*(?:\#[^\n]*)?\n"
        r"(?P=indent)[ \t]+import asyncio\s*(?:\#[^\n]*)?\n"
        r"(?P=indent)except ImportError:\s*(?:\#[^\n]*)?\n"
        r"(?:(?P=indent)[ \t]+[^\n]*\n?)*",
        re.MULTILINE,
    )
    new_content = well_formed.sub("", content)
    if new_content != content:
        # Collapse runs of 3+ blank lines left behind by the removal.
        new_content = re.sub(r"\n{3,}", "\n\n", new_content)
    return new_content


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
            updated = _fix_pytest_mark_asyncio_body(updated)
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
        # Skip async-only directories (e.g. the fastapi-redis-sdk bridge)
        # so that no broken sync mirror is generated.
        if any(
            root.endswith(os.sep + d) or root == str(repo_root / d)
            for d in _ASYNC_ONLY_DIRS
        ):
            continue
        for filename in filenames:
            if filename.rpartition(".")[-1] in (
                "py",
                "pyi",
            ):
                # Skip tests that target async-only modules.
                if filename in _ASYNC_ONLY_TEST_BASENAMES:
                    continue
                filepaths.append(os.path.join(root, filename))

    unasync.unasync_files(filepaths, rules)
    apply_post_sync_fixes(repo_root)


if __name__ == "__main__":
    main()
