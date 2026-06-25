# type: ignore
"""Tests for KeyspaceEvents constants and enable_keyspace_events helper."""

import pytest

from aredis_om import get_redis_connection
from aredis_om.model.keyspace_events import (
    KeyspaceEvents,
    build_flags,
    disable_keyspace_events,
    enable_keyspace_events,
)

from .conftest import py_test_mark_asyncio


class TestConstants:
    def test_prefix_constants(self):
        assert KeyspaceEvents.KEYSPACE_PREFIX == "K"
        assert KeyspaceEvents.KEYEVENT_PREFIX == "E"
        assert KeyspaceEvents.ALL_KEY_EVENTS_ALIAS == "A"

    def test_category_constants(self):
        assert KeyspaceEvents.GENERIC_COMMANDS == "g"
        assert KeyspaceEvents.STRING_COMMANDS == "$"
        assert KeyspaceEvents.LIST_COMMANDS == "l"
        assert KeyspaceEvents.SET_COMMANDS == "s"
        assert KeyspaceEvents.HASH_COMMANDS == "h"
        assert KeyspaceEvents.SORTED_SET_COMMANDS == "z"
        assert KeyspaceEvents.STREAM_COMMANDS == "t"

    def test_special_constants(self):
        assert KeyspaceEvents.EXPIRED_EVENTS == "x"
        assert KeyspaceEvents.EVICTED_EVENTS == "e"
        assert KeyspaceEvents.KEY_MISS_EVENTS == "m"
        assert KeyspaceEvents.NEW_KEY_EVENTS == "n"

    def test_presets(self):
        assert KeyspaceEvents.ALL_EVENTS_PRESET == "AKE"
        assert KeyspaceEvents.EXPIRATIONS_PRESET == "Ex"


class TestBuildFlags:
    def test_basic_combination(self):
        flags = build_flags(
            KeyspaceEvents.KEYSPACE_PREFIX,
            KeyspaceEvents.GENERIC_COMMANDS,
            KeyspaceEvents.EXPIRED_EVENTS,
        )
        assert "".join(sorted(flags)) == "Kgx"

    def test_removes_duplicates(self):
        flags = build_flags(
            KeyspaceEvents.GENERIC_COMMANDS,
            KeyspaceEvents.GENERIC_COMMANDS,
        )
        assert flags == "g"

    def test_accepts_pre_joined_string(self):
            flags = build_flags("KE", "g", "KEx")
            # KEK, Eg, EKgx... deduplicated: K, E, g, x
            assert "".join(sorted(flags)) == "EKgx"

    def test_empty(self):
        assert build_flags() == ""

    def test_preset(self):
        flags = build_flags(KeyspaceEvents.ALL_EVENTS_PRESET)
        # "AKE" → sorted: AEK
        assert "".join(sorted(flags)) == "AEK"


class TestEnableDisable:
    @py_test_mark_asyncio
    async def test_enable_and_disable(self, key_prefix):
        db = get_redis_connection()
        # Set a known flag combo.
        await enable_keyspace_events(db, "Kgx")
        try:
            current = await db.config_get("notify-keyspace-events")
            # The server may report flags in uppercase or lowercase.
            value = (current.get("notify-keyspace-events") or "").upper()
            # Allow for ALIAS 'A' expanding.
            assert "K" in value or "AKE" in value or "E" in value
        finally:
            await disable_keyspace_events(db)

    @py_test_mark_asyncio
    async def test_disable_clears(self):
        db = get_redis_connection()
        # Enable then disable.
        await enable_keyspace_events(db, "Kgx")
        await disable_keyspace_events(db)
        current = await db.config_get("notify-keyspace-events")
        assert current.get("notify-keyspace-events") in ("", None)
