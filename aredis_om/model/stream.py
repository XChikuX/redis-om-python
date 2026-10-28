"""Redis Streams helper with Redis 7.4–8.10 extensions.

Redis Streams are a core data type that redis-om-python did not previously
expose at the helper level.  This module provides a thin, typed wrapper
around the ``X*`` command family, including several newer commands that
were previously unsupported:

* **Redis 8.2** — ``XACKDEL`` and ``XDELEX`` (ack-and-delete /
  delete-by-id strategies: ``DELREF``, ``ACKED``).
* **Redis 8.4** — ``XREADGROUP ... CLAIM <min-idle>`` for consuming idle
  pending entries *and* new incoming entries in a single call.
* **Redis 8.6** — ``XADD ... IDMP`` / ``IDMPAUTO`` for at-most-once
  idempotent production: the server deduplicates appends by
  ``(producer-id, idempotent-id)`` within a per-stream retention window
  (see ``configure_idempotency()``).
* **Redis 8.8** — ``XNACK`` for explicitly releasing pending messages
  without acknowledging them.
* **Redis 8.10** — ``XREAD``/``XREADGROUP ... MAXCOUNT``/``MAXSIZE`` for
  capping the cumulative reply across all streams in a single call.

All methods use ``execute_command`` so they work on any redis-py client
without depending on the version-bound high-level method bindings.

Example — producer/consumer::

    from aredis_om import RedisStream

    stream = RedisStream(db, "orders")

    # Idempotent produce (Redis 8.6+): retries are safe.  The server
    # deduplicates by producer id + a hash of the entry fields.
    await stream.add({"event": "created"}, idempotent=True)

    # Read and process in a consumer group.
    for entry in await stream.read_group("workers", "w1", count=10):
        await process(entry)
        await stream.ack_and_delete("workers", entry.id)

The class degrades gracefully on older servers: unsupported options are
dropped with a one-time ``UserWarning`` per connection, and ``XACKDEL`` /
``XDELEX`` fall back to ``XACK`` + ``XDEL``.
"""

import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import warnings

# Cache: connection id → set of unsupported command names.  We probe once
# per connection so we don't repeat ``COMMAND INFO`` round trips.
_capability_cache: "Dict[int, set]" = {}
# Cache: connection id → whether XREAD/XREADGROUP accept MAXCOUNT/MAXSIZE.
_read_limits_cache: "Dict[int, bool]" = {}
# Cache: connection id → whether XADD accepts IDMP/IDMPAUTO.
_idmp_cache: "Dict[int, bool]" = {}
# Connection ids already warned about missing MAXCOUNT/MAXSIZE support.
_read_limits_warned: "set" = set()
# Connection ids already warned about missing IDMP/IDMPAUTO support.
_idmp_warned: "set" = set()

# Stable per-process default producer id for ``add(idempotent=True)``.
# Retries within one process deduplicate against each other; pass an
# explicit ``producer_id`` for dedup across process restarts.
_DEFAULT_PRODUCER_ID = uuid.uuid4().hex


def clear_stream_capability_cache() -> None:
    """Clear the cached stream capability results (for testing)."""
    _capability_cache.clear()
    _read_limits_cache.clear()
    _idmp_cache.clear()
    _read_limits_warned.clear()
    _idmp_warned.clear()


def _warn_read_limits_unsupported(conn_id: int) -> None:
    """Warn once per connection that MAXCOUNT/MAXSIZE are being dropped."""
    if conn_id not in _read_limits_warned:
        _read_limits_warned.add(conn_id)
        warnings.warn(
            "RedisStream: server does not support XREAD/XREADGROUP "
            "MAXCOUNT/MAXSIZE (requires Redis 8.10+); the read caps are "
            "ignored for this connection.",
            UserWarning,
            stacklevel=3,
        )


def _warn_idmp_unsupported(conn_id: int) -> None:
    """Warn once per connection that IDMP/IDMPAUTO is being dropped."""
    if conn_id not in _idmp_warned:
        _idmp_warned.add(conn_id)
        warnings.warn(
            "RedisStream: server does not support XADD IDMP/IDMPAUTO "
            "(requires Redis 8.6+); the entry is appended without "
            "idempotency for this connection.",
            UserWarning,
            stacklevel=3,
        )


def _conn_id(db: Any) -> int:
    """Identity of a connection.  A helper because ``RedisStream.add``
    shadows the ``id`` builtin with its entry-id parameter."""
    return id(db)


class StreamEntry:
    """A single stream entry (id + field/value mapping)."""

    __slots__ = ("id", "fields")

    def __init__(self, entry_id: str, fields: Mapping[str, Any]):
        self.id = entry_id
        self.fields = dict(fields)

    def __repr__(self) -> str:
        return f"StreamEntry(id={self.id!r}, fields={self.fields!r})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, StreamEntry)
            and self.id == other.id
            and self.fields == other.fields
        )


class RedisStream:
    """A typed helper around the Redis ``X*`` stream command family.

    Wraps the standard stream commands plus the newer
    ``XACKDEL``/``XDELEX``/``XNACK``/``IDMP`` extensions with graceful
    fallback on older servers.
    """

    def __init__(self, db: Any, key: str):
        self._db = db
        self._key = key

    @property
    def key(self) -> str:
        return self._key

    # ── capability probing ─────────────────────────────────────────

    async def _supports(self, command: str) -> bool:
        """Return ``True`` when the server supports *command*.

        Probes ``COMMAND INFO`` once per connection and caches the result.
        """
        conn_id = id(self._db)
        missing = _capability_cache.setdefault(conn_id, set())
        if command in missing:
            return False
        # If we've already confirmed support, ``missing`` won't contain it.
        # Check the inverse: cache only *missing* commands.  We can't store
        # positive results cheaply without a second set, so re-probe only
        # happens for commands we *haven't* marked missing.
        try:
            info = await self._db.execute_command("COMMAND", "INFO", command)
            if not info or not all(info):
                missing.add(command)
                return False
            return True
        except Exception:
            missing.add(command)
            return False

    async def _supports_read_limits(self) -> bool:
        """Return ``True`` when ``XREAD``/``XREADGROUP`` accept
        ``MAXCOUNT``/``MAXSIZE`` (Redis 8.10+).

        Both arguments shipped in the same release, so a single probe
        covers both commands.  ``COMMAND INFO`` cannot be used here:
        ``MAXCOUNT``/``MAXSIZE`` are arguments, not commands —
        ``COMMAND INFO xread.maxcount`` returns ``nil`` on every server
        version.  Instead we execute a harmless ``XREAD ... MAXCOUNT``
        against this stream key: ``XREAD`` never creates keys and ``$``
        returns immediately with no entries.
        """
        conn_id = id(self._db)
        cached = _read_limits_cache.get(conn_id)
        if cached is not None:
            return cached
        try:
            await self._db.execute_command(
                "XREAD", "MAXCOUNT", 1, "STREAMS", self._key, "$"
            )
            supported = True
        except Exception:
            supported = False
        _read_limits_cache[conn_id] = supported
        return supported

    async def _supports_idmp(self) -> bool:
        """Return ``True`` when ``XADD`` accepts ``IDMP``/``IDMPAUTO``
        (Redis 8.6+).

        ``COMMAND INFO`` cannot detect this — ``IDMP`` is an argument,
        not a command (``COMMAND INFO xadd.idmp`` returns ``nil`` on every
        server), and redis-py's ``COMMAND`` callback cannot parse
        ``COMMAND DOCS`` replies.  Instead we execute a side-effect-free
        trial ``XADD`` against a sibling key that does not exist:
        ``NOMKSTREAM`` makes the server return ``nil`` without creating
        the key (verified against Redis 8.10), and because the stream
        object does not exist the trial cannot register idempotency
        state.  Pre-8.6 servers reject the misparsed ``IDMP`` token with
        "Invalid stream ID" (verified against Redis 8.4).
        """
        conn_id = id(self._db)
        cached = _idmp_cache.get(conn_id)
        if cached is not None:
            return cached
        try:
            await self._db.execute_command(
                "XADD",
                f"{self._key}:__idmp_probe__",
                "NOMKSTREAM",
                "IDMPAUTO",
                "probe",
                "*",
                "probe",
                "probe",
            )
            supported = True
        except Exception:
            supported = False
        _idmp_cache[conn_id] = supported
        return supported

    # ── producing ──────────────────────────────────────────────────

    async def add(
        self,
        fields: Mapping[str, Any],
        *,
        id: str = "*",
        maxlen: Optional[int] = None,
        approximate: bool = True,
        minid: Optional[str] = None,
        idempotent: bool = False,
        producer_id: Optional[str] = None,
        idempotent_id: Optional[str] = None,
    ) -> str:
        """``XADD`` — append an entry, returning the new entry id.

        Args:
            fields: Field/value mapping for the entry.
            id: Entry id.  ``*`` (default) lets Redis assign one.  Explicit
                ids cannot be combined with ``idempotent`` (server
                restriction: IDMP requires auto-generated ids).
            maxlen: Trim the stream to at most this many entries.
            approximate: Use approximate trimming (``~``) — faster.
            minid: Trim entries with ids lower than this.
            idempotent: Enable at-most-once idempotency (Redis 8.6+).
                Retrying the same append returns the original entry id
                instead of duplicating the entry.  Without an explicit
                ``idempotent_id`` the server derives the dedup key from
                the entry fields (``IDMPAUTO``), so identical fields
                deduplicate.  Ignored (with a one-time ``UserWarning``)
                on servers older than 8.6.
            producer_id: Stable producer identity for dedup.  Defaults to
                a per-process id, so retries deduplicate within this
                process only; pass an explicit id to deduplicate across
                process restarts.
            idempotent_id: Explicit dedup key (uses ``IDMP`` instead of
                ``IDMPAUTO``).  Retries with the same
                ``(producer_id, idempotent_id)`` pair return the original
                entry id.

        Returns:
            The entry id assigned by Redis (or the original id on a
            deduplicated retry).

        Raises:
            ValueError: When ``idempotent`` is combined with an explicit
                entry ``id`` (the server requires auto-generated ids).
        """
        use_idmp = idempotent
        if use_idmp:
            if id != "*":
                raise ValueError(
                    "idempotent=True requires id='*' (the server only "
                    "supports IDMP/IDMPAUTO with auto-generated ids)"
                )
            if await self._supports_idmp():
                pid = producer_id if producer_id is not None else _DEFAULT_PRODUCER_ID
            else:
                use_idmp = False
                _warn_idmp_unsupported(_conn_id(self._db))

        args: List[Any] = ["XADD", self._key]
        if use_idmp:
            if idempotent_id is not None:
                args.extend(["IDMP", pid, str(idempotent_id)])
            else:
                args.extend(["IDMPAUTO", pid])
        if maxlen is not None:
            args.append("MAXLEN")
            if approximate:
                args.append("~")
            args.append(maxlen)
        if minid is not None:
            args.append("MINID")
            if approximate:
                args.append("~")
            args.append(minid)

        args.append(id)
        for k, v in fields.items():
            args.extend([k, v])
        return await self._db.execute_command(*args)

    async def configure_idempotency(
        self,
        duration: Optional[int] = None,
        maxsize: Optional[int] = None,
    ) -> bool:
        """``XCFGSET`` — configure the per-stream idempotency window.

        Controls how long the server remembers ``(producer-id,
        idempotent-id)`` pairs for ``add(idempotent=True)`` dedup.

        Args:
            duration: Retention window in seconds (1-86400; server
                default 100).
            maxsize: Maximum remembered ids per producer (1-10000;
                server default 1000).

        Returns:
            ``True`` on success.  Raises ``ResponseError`` when the
            stream does not exist or a value is out of bounds (the
            stream must already exist — append at least one entry
            first).
        """
        if duration is None and maxsize is None:
            raise ValueError("configure_idempotency requires duration and/or maxsize")
        args: List[Any] = ["XCFGSET", self._key]
        if duration is not None:
            args.extend(["IDMP-DURATION", duration])
        if maxsize is not None:
            args.extend(["IDMP-MAXSIZE", maxsize])
        await self._db.execute_command(*args)
        return True

    # ── reading ────────────────────────────────────────────────────

    async def read(
        self,
        *,
        count: Optional[int] = None,
        block: Optional[int] = None,
        last_id: str = "$",
        max_count: Optional[int] = None,
        max_size: Optional[int] = None,
    ) -> List[StreamEntry]:
        """``XREAD`` — read new entries from the stream.

        Args:
            count: Maximum number of entries to return *per stream*.
            block: Milliseconds to block waiting for new entries (``None``
                → non-blocking).
            last_id: Start reading after this id.  ``$`` (default) reads
                only new entries.  Use ``"0"`` to read from the start.
            max_count: Redis 8.10+ ``MAXCOUNT`` — cap the cumulative
                number of entries returned across *all* streams in the
                reply.  Unlike ``count`` (which applies per stream),
                ``max_count`` bounds the total.  Must be greater than or
                equal to ``count`` when both are given.
            max_size: Redis 8.10+ ``MAXSIZE`` — cap the cumulative reply
                size in bytes across *all* streams.  The limit is checked
                before delivering each entry, so an entry that would
                exceed it is neither returned nor added to a consumer
                group's PEL.

        Returns:
            A list of :class:`StreamEntry`.

        Note:
            ``max_count``/``max_size`` require Redis 8.10+.  On older
            servers they are ignored (a single-stream ``count`` still
            applies).
        """
        args: List[Any] = ["XREAD"]
        if count is not None:
            args.extend(["COUNT", count])
        if max_count is not None or max_size is not None:
            if await self._supports_read_limits():
                if max_count is not None:
                    args.extend(["MAXCOUNT", max_count])
                if max_size is not None:
                    args.extend(["MAXSIZE", max_size])
            else:
                _warn_read_limits_unsupported(id(self._db))
        if block is not None:
            args.extend(["BLOCK", block])
        args.extend(["STREAMS", self._key, last_id])
        raw = await self._db.execute_command(*args)
        return _parse_read_response(raw, self._key)

    async def read_group(
        self,
        group: str,
        consumer: str,
        *,
        count: Optional[int] = None,
        block: Optional[int] = None,
        noack: bool = False,
        claim_min_idle: Optional[int] = None,
        max_count: Optional[int] = None,
        max_size: Optional[int] = None,
    ) -> List[StreamEntry]:
        """``XREADGROUP`` — read entries for a consumer group.

        Args:
            group: Consumer group name.
            consumer: Consumer name within the group.
            count: Maximum number of entries to return *per stream*.
            block: Milliseconds to block waiting for new entries.
            noack: Skip adding entries to the PEL (auto-acknowledge).
            claim_min_idle: Redis 8.4+ ``CLAIM`` option.  When set,
                return both idle pending entries older than this many
                milliseconds *and* new incoming entries in a single call.
            max_count: Redis 8.10+ ``MAXCOUNT`` — cap the cumulative
                number of entries returned across *all* streams in the
                reply.  Unlike ``count`` (which applies per stream),
                ``max_count`` bounds the total.  Must be greater than or
                equal to ``count`` when both are given.
            max_size: Redis 8.10+ ``MAXSIZE`` — cap the cumulative reply
                size in bytes across *all* streams.  The limit is checked
                before delivering each entry, so an entry that would
                exceed it is neither returned nor added to the PEL.

        Returns:
            A list of :class:`StreamEntry`.

        Note:
            ``max_count``/``max_size`` require Redis 8.10+.  On older
            servers they are ignored (a single-stream ``count`` still
            applies).
        """
        args: List[Any] = ["XREADGROUP", "GROUP", group, consumer]
        if count is not None:
            args.extend(["COUNT", count])
        if max_count is not None or max_size is not None:
            if await self._supports_read_limits():
                if max_count is not None:
                    args.extend(["MAXCOUNT", max_count])
                if max_size is not None:
                    args.extend(["MAXSIZE", max_size])
            else:
                _warn_read_limits_unsupported(id(self._db))
        if block is not None:
            args.extend(["BLOCK", block])
        if noack:
            args.append("NOACK")
        if claim_min_idle is not None:
            args.extend(["CLAIM", claim_min_idle])
        args.extend(["STREAMS", self._key, ">"])
        raw = await self._db.execute_command(*args)
        return _parse_read_response(raw, self._key)

    # ── consumer groups ────────────────────────────────────────────

    async def create_group(
        self,
        group: str,
        *,
        id: str = "$",
        mkstream: bool = False,
    ) -> bool:
        """``XGROUP CREATE`` — create a consumer group."""
        args: List[Any] = ["XGROUP", "CREATE", self._key, group, id]
        if mkstream:
            args.append("MKSTREAM")
        try:
            await self._db.execute_command(*args)
            return True
        except Exception as e:
            if "BUSYGROUP" in str(e):
                return False
            raise

    async def destroy_group(self, group: str) -> int:
        """``XGROUP DESTROY`` — destroy a consumer group."""
        return await self._db.execute_command("XGROUP", "DESTROY", self._key, group)

    # ── acknowledging & deleting ───────────────────────────────────

    async def ack(self, group: str, *ids: str) -> int:
        """``XACK`` — acknowledge entries in a consumer group."""
        return await self._db.execute_command("XACK", self._key, group, *ids)

    async def delete(self, *ids: str) -> int:
        """``XDEL`` — delete entries by id."""
        return await self._db.execute_command("XDEL", self._key, *ids)

    async def ack_and_delete(
        self,
        group: str,
        *ids: str,
        strategy: Optional[str] = None,
    ) -> int:
        """Acknowledge and delete entries atomically.

        Uses ``XACKDEL`` (Redis 8.2+) when available for a single round
        trip.  Falls back to ``XACK`` + ``XDEL`` on older servers.

        Args:
            group: Consumer group name.
            *ids: Entry ids to acknowledge and delete.
            strategy: Optional trimming strategy for ``XACKDEL``:
                ``"DELREF"`` or ``"ACKED"``.  Ignored on fallback.

        Returns:
            The number of entries acknowledged (from ``XACK``).
        """
        if await self._supports("xackdel"):
            args: List[Any] = ["XACKDEL", self._key, group]
            if strategy is not None:
                args.append(strategy.upper())
            args.extend(["IDS", str(len(ids)), *ids])
            raw = await self._db.execute_command(*args)
            # XACKDEL returns [ack_count, del_count]; we return ack_count.
            if isinstance(raw, list) and raw:
                return int(raw[0])
            return int(raw)
        # Fallback: two commands (non-atomic).
        acked = await self.ack(group, *ids)
        if ids:
            await self.delete(*ids)
        return acked

    async def delete_ex(
        self,
        *ids: str,
        strategy: Optional[str] = None,
    ) -> int:
        """``XDELEX`` — delete entries with an optional strategy.

        Uses ``XDELEX`` (Redis 8.2+) when available.  Falls back to
        ``XDEL`` on older servers (strategy is ignored).

        Args:
            *ids: Entry ids to delete.
            strategy: ``"DELREF"`` or ``"ACKED"`` (Redis 8.2+).

        Returns:
            The number of entries deleted.
        """
        if await self._supports("xdelex"):
            args: List[Any] = ["XDELEX", self._key]
            if strategy is not None:
                args.append(strategy.upper())
            args.extend(["IDS", str(len(ids)), *ids])
            raw = await self._db.execute_command(*args)
            return int(raw) if not isinstance(raw, list) else int(raw[0])
        return await self.delete(*ids)

    async def nack(
        self,
        group: str,
        consumer: str,
        *ids: str,
        retry: bool = False,
    ) -> int:
        """``XNACK`` — release pending messages without acknowledging.

        Uses ``XNACK`` (Redis 8.8+).  Falls back to ``XPENDING``-based
        noop on older servers (returns ``0``).

        Args:
            group: Consumer group name.
            consumer: Consumer that currently owns the messages.
            *ids: Entry ids to release.
            retry: If ``True``, mark the entries for redelivery
                immediately rather than leaving them idle.

        Returns:
            The number of entries released.
        """
        if await self._supports("xnack"):
            args: List[Any] = [
                "XNACK",
                self._key,
                group,
                "SILENT",
                "IDS",
                str(len(ids)),
                *ids,
            ]
            if retry:
                args.append("RETRYCOUNT")
                args.append("0")
            return await self._db.execute_command(*args)
        # No graceful server-side equivalent on older Redis.
        return 0

    # ── claiming ───────────────────────────────────────────────────

    async def claim(
        self,
        group: str,
        consumer: str,
        min_idle_time: int,
        *ids: str,
        justid: bool = False,
    ) -> List[StreamEntry]:
        """``XCLAIM`` — claim pending entries for a different consumer."""
        args: List[Any] = [
            "XCLAIM",
            self._key,
            group,
            consumer,
            min_idle_time,
            *ids,
        ]
        if justid:
            args.append("JUSTID")
        raw = await self._db.execute_command(*args)
        if justid:
            # XCLAIM ... JUSTID returns a flat list of id strings, but
            # redis-py's response callback may post-process it into
            # ``(id, fields)`` tuples (with empty fields).  Handle both.
            entries: List[StreamEntry] = []
            if raw and isinstance(raw, list):
                for item in raw:
                    if isinstance(item, tuple):
                        entries.append(StreamEntry(item[0], item[1] or {}))
                    elif isinstance(item, list):
                        entries.append(
                            StreamEntry(item[0], dict(item[1]) if item[1:] else {})
                        )
                    else:
                        entries.append(StreamEntry(item, {}))
            return entries
        return _parse_entries(raw)

    # ── trimming ───────────────────────────────────────────────────

    async def trim(
        self,
        maxlen: Optional[int] = None,
        *,
        minid: Optional[str] = None,
        approximate: bool = True,
        strategy: Optional[str] = None,
    ) -> int:
        """``XTRIM`` — trim the stream.

        Args:
            maxlen: Maximum number of entries to keep.
            minid: Trim entries with ids lower than this.
            approximate: Use approximate trimming (``~``).
            strategy: Redis 8.2+ strategy: ``"DELREF"`` or ``"ACKED"``.

        Returns:
            The number of entries removed.
        """
        args: List[Any] = ["XTRIM", self._key]
        if strategy is not None and await self._supports("xtrim.strategy"):
            args.append(strategy.upper())
        if maxlen is not None:
            args.extend(["MAXLEN"])
            if approximate:
                args.append("~")
            args.append(maxlen)
        if minid is not None:
            args.append("MINID")
            if approximate:
                args.append("~")
            args.append(minid)
        return await self._db.execute_command(*args)

    # ── introspection ──────────────────────────────────────────────

    async def length(self) -> int:
        """``XLEN`` — number of entries in the stream."""
        return await self._db.execute_command("XLEN", self._key)

    async def range(
        self,
        start: str = "-",
        end: str = "+",
        count: Optional[int] = None,
    ) -> List[StreamEntry]:
        """``XRANGE`` — entries within an id range."""
        args: List[Any] = ["XRANGE", self._key, start, end]
        if count is not None:
            args.extend(["COUNT", count])
        return _parse_entries(await self._db.execute_command(*args))

    async def revrange(
        self,
        start: str = "+",
        end: str = "-",
        count: Optional[int] = None,
    ) -> List[StreamEntry]:
        """``XREVRANGE`` — entries in reverse id order."""
        args: List[Any] = ["XREVRANGE", self._key, start, end]
        if count is not None:
            args.extend(["COUNT", count])
        return _parse_entries(await self._db.execute_command(*args))

    async def info(self) -> Dict[str, Any]:
        """``XINFO STREAM`` — stream metadata (flat dict)."""
        raw = await self._db.execute_command("XINFO", "STREAM", self._key)
        return _flatten_pairs(raw)


def _parse_read_response(raw: Any, key: str) -> List[StreamEntry]:
    """Parse the ``XREAD``/``XREADGROUP`` response for a single stream.

    Handles the reply shapes redis-py's response callbacks produce in
    every protocol/decoding combination:

    - RESP2: ``[[stream_key, [(id, fields), ...]], ...]``
    - RESP3: ``{stream_key: [[(id, fields), ...]]}`` (dict reply with a
      list-wrapped entries list)
    - ``decode_responses=False``: stream keys (and ids/field names) as
      ``bytes``
    """
    if not raw:
        return []
    if isinstance(raw, dict):  # RESP3
        for stream_key, entries in raw.items():
            if _key_matches(stream_key, key):
                return _parse_entries(_unwrap_entries(entries))
        return []
    for stream_key, entries in raw:  # RESP2: [[key, entries], ...]
        if _key_matches(stream_key, key):
            return _parse_entries(_unwrap_entries(entries))
    # Fallback: if response shape differs, parse what we can.
    return _parse_entries(raw)


def _key_matches(stream_key: Any, key: str) -> bool:
    """Compare a (possibly ``bytes``) reply stream key against *key*."""
    if isinstance(stream_key, bytes):
        return stream_key.decode("utf-8", "replace") == key
    return stream_key == key


def _unwrap_entries(entries: Any) -> Any:
    """Unwrap the extra list nesting redis-py's RESP3 ``XREAD`` callback
    produces (``[[(id, fields), ...]]`` vs RESP2's ``[(id, fields), ...]``)."""
    if isinstance(entries, list) and len(entries) == 1 and isinstance(entries[0], list):
        return entries[0]
    return entries


def _parse_entries(raw: Any) -> List[StreamEntry]:
    """Parse a list of ``[id, [k, v, ...]]`` entries."""
    entries: List[StreamEntry] = []
    if not raw:
        return entries
    for entry in raw:
        # redis-py pads missing XCLAIM results with ``(None, None)``.
        if entry is None:
            continue
        entry_id, fields_raw = entry
        if entry_id is None:
            continue
        fields: Dict[str, Any] = {}
        if isinstance(fields_raw, list):
            for i in range(0, len(fields_raw) - 1, 2):
                fields[fields_raw[i]] = fields_raw[i + 1]
        elif isinstance(fields_raw, dict):
            fields = dict(fields_raw)
        entries.append(StreamEntry(entry_id, fields))
    return entries


def _flatten_pairs(raw: Any) -> Dict[str, Any]:
    """Flatten a flat ``[k1, v1, k2, v2, ...]`` or nested list into a dict."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list) and raw and isinstance(raw[0], list) and len(raw[0]) == 2:
        # nested [[k, v], ...]
        return {k: v for k, v in raw}
    if isinstance(raw, list) and len(raw) % 2 == 0:
        return {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
    return {}
