"""Async Redis/Valkey client import shim.

Prefers ``valkey`` (the official Valkey Python client) when it is
installed because it handles Valkey server quirks such as the flat-list
``FT.INFO`` RESP3 response shape more naturally than ``redis-py``.

``redis`` remains a fallback when ``valkey`` is not installed, so this
package keeps working on plain Redis servers unchanged.
"""

try:
    from valkey import asyncio as redis  # type: ignore[no-redef]
except ImportError:  # pragma: no cover - exercised only without valkey installed
    from redis import asyncio as redis  # type: ignore[no-redef]
