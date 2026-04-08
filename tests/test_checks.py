# type: ignore
"""Tests for aredis_om.checks – command check caching and feature detection."""

import asyncio

import pytest

from aredis_om.checks import check_for_command, clear_command_cache

from .conftest import py_test_mark_asyncio


class FakeConn:
    """A minimal fake Redis connection for testing command checks."""

    def __init__(self, responses=None, raise_auth_error=False):
        self.calls = []
        self.responses = responses or {}
        self.raise_auth_error = raise_auth_error

    async def execute_command(self, *args):
        self.calls.append(args)
        cmd = args[2] if len(args) > 2 else args[0]
        if self.raise_auth_error:
            from redis.exceptions import AuthenticationError

            raise AuthenticationError("no auth")
        return self.responses.get(cmd, [True])


# ---------------------------------------------------------------------------
# check_for_command
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_check_for_command_returns_true():
    clear_command_cache()
    conn = FakeConn(responses={"json.set": [True]})
    result = await check_for_command(conn, "json.set")
    assert result is True


@py_test_mark_asyncio
async def test_check_for_command_returns_false_for_missing():
    clear_command_cache()
    conn = FakeConn(responses={"badcmd": [None]})
    result = await check_for_command(conn, "badcmd")
    assert result is False


@py_test_mark_asyncio
async def test_check_for_command_caches_result():
    clear_command_cache()
    conn = FakeConn(responses={"ft.search": [True]})
    # First call
    await check_for_command(conn, "ft.search")
    # Second call should use cache (no new execute_command call)
    await check_for_command(conn, "ft.search")
    assert len(conn.calls) == 1


@py_test_mark_asyncio
async def test_check_for_command_returns_false_on_auth_error():
    clear_command_cache()
    conn = FakeConn(raise_auth_error=True)
    result = await check_for_command(conn, "ft.search")
    assert result is False


@py_test_mark_asyncio
async def test_clear_command_cache_resets():
    conn = FakeConn(responses={"json.set": [True]})
    await check_for_command(conn, "json.set")
    assert len(conn.calls) == 1

    clear_command_cache()

    await check_for_command(conn, "json.set")
    assert len(conn.calls) == 2


@py_test_mark_asyncio
async def test_check_for_command_different_commands_same_conn():
    clear_command_cache()
    conn = FakeConn(responses={"json.set": [True], "ft.search": [True]})
    r1 = await check_for_command(conn, "json.set")
    r2 = await check_for_command(conn, "ft.search")
    assert r1 is True
    assert r2 is True
    assert len(conn.calls) == 2


@py_test_mark_asyncio
async def test_check_for_command_different_conns_not_shared():
    clear_command_cache()
    conn1 = FakeConn(responses={"json.set": [True]})
    conn2 = FakeConn(responses={"json.set": [True]})
    await check_for_command(conn1, "json.set")
    await check_for_command(conn2, "json.set")
    # Each conn should have its own call
    assert len(conn1.calls) == 1
    assert len(conn2.calls) == 1
