import asyncio
import hashlib
import importlib
import json
import logging
import pkgutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from ... import redis

log = logging.getLogger(__name__)

# Redis key holding a chronological JSON log of applied migrations.
MIGRATION_HISTORY_KEY = "redis_om:migration_history"

# Length of the schema-hash suffix appended to physical index names when
# alias-based migrations are enabled. Eight hex chars (32 bits) of SHA-1
# is ample to avoid collisions between successive schema revisions of the
# same model.
PHYSICAL_INDEX_HASH_LEN = 8


class MigrationError(Exception):
    pass


def import_submodules(root_module_name: str):
    """Import all submodules of a module, recursively.

    ``pkgutil.walk_packages`` requires a concrete package name to traverse,
    so a root module name is mandatory. Pass ``module=None`` to ``Migrator``
    to skip automatic submodule import entirely.
    """
    root_module = importlib.import_module(root_module_name)

    if not hasattr(root_module, "__path__"):
        raise MigrationError(
            "The root module must be a Python package. "
            f"You specified: {root_module_name}"
        )

    for loader, module_name, is_pkg in pkgutil.walk_packages(
        root_module.__path__, root_module.__name__ + "."
    ):
        importlib.import_module(module_name)


def schema_hash_key(index_name):
    return f"{index_name}:hash"


def physical_index_name(alias_name: str, schema_hash: str) -> str:
    """Derive a versioned physical index name from an alias + schema hash.

    The alias is the user-facing name (``Meta.index_name``) and the physical
    index is what RediSearch actually indexes documents into. Queries target
    the alias via ``FT.ALIASUPDATE``, so swapping the alias from one physical
    index to another is the zero-downtime migration primitive.
    """
    short_hash = schema_hash[:PHYSICAL_INDEX_HASH_LEN]
    return f"{alias_name}__v{short_hash}"


async def _create_index_cluster(
    conn: redis.RedisCluster, index_name, schema, current_hash
):
    """Create a search index on a Redis Cluster.

    In Redis 8+, search indexes are cluster-aware and automatically
    distributed across all shards.  We send the ``FT.CREATE`` command to a
    single random node; the cluster takes care of propagation.  If the
    command must target a specific node (older module builds), we fall back
    to sending it to each primary individually, tolerating "Index already
    exists" errors from nodes that received the index via replication.
    """
    try:
        await conn.ft(index_name).info()
    except redis.ResponseError:
        command = f"ft.create {index_name} {schema}".split()
        try:
            # Redis 8: send to a single node – cluster propagates internally.
            await conn.execute_command(*command, target_nodes=redis.RedisCluster.RANDOM)
        except redis.ResponseError as exc:
            if "Index already exists" not in str(exc):
                raise
        await conn.set(schema_hash_key(index_name), current_hash)
    else:
        log.info("Index already exists, skipping. Index hash: %s", index_name)


async def _wait_for_index(conn, index_name: str, timeout: float = 3.0) -> None:
    """Poll FT.INFO until the index reports it is fully indexed.

    Redis 8.8+ introduced asynchronous background indexing for existing
    documents, which means ``FT.CREATE`` can return before previously
    written keys have been scanned. Tests that create an index and
    immediately search it can see empty or partial results unless we
    wait for indexing to finish.

    The index may also not be visible immediately when another
    concurrent process (e.g. another pytest-xdist worker) created it
    between our ``FT.INFO`` existence check and ``FT.CREATE``. In that
    case we keep polling instead of returning early so that callers can
    rely on the index being queryable once this function returns.

    3 s is the empirically sufficient budget for both standalone and
    pytest-xdist runs (20 workers, single Redis 8 instance) without
    turning transient load spikes into minute-long suite hangs.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    last_error: Optional[Exception] = None

    while asyncio.get_event_loop().time() < deadline:
        try:
            info = await conn.ft(index_name).info()
        except redis.ResponseError as exc:
            # Index not visible yet (race with a concurrent creator). Keep
            # polling until it appears or we time out.
            last_error = exc
            await asyncio.sleep(0.05)
            continue

        last_error = None
        raw_percent = info.get("percent_indexed")

        if raw_percent is not None:
            try:
                # Converts "1", "1.0", 1, etc., cleanly to a float
                if float(raw_percent) >= 1.0:
                    return
            except (ValueError, TypeError):
                # Handle edge cases where percent_indexed isn't a valid number
                pass

        await asyncio.sleep(0.05)

    if last_error is not None:
        log.warning(
            "Index %s never became queryable after %ss: %s",
            index_name,
            timeout,
            last_error,
        )
    else:
        log.warning("Timeout waiting for index %s to reach 100%% indexed", index_name)


async def _retry_aliasupdate(
    conn, physical: str, alias: str, attempts: int = 30
) -> None:
    """Run ``FT.ALIASUPDATE`` with retry on transient ``SEARCH_INDEX_NOT_FOUND``.

    The companion ``ALIAS_CREATE_INDEX`` action builds the physical index
    immediately before the alias-link / alias-swap / alias-adopt action runs
    in the same migrator. In rare cases the Redis search module can take a
    moment to make a freshly-created index visible across all internal
    structures, causing an ``FT.ALIASUPDATE`` sent microseconds later to
    surface ``SEARCH_INDEX_NOT_FOUND`` for the physical index. The index is
    not actually gone — it just isn't queryable yet.

    A short polling retry avoids turning that transient visibility race into
    a hard migration failure. ``FT.ALIASUPDATE`` itself is idempotent: repeating
    it with the same physical and alias is a no-op once the alias is in place.

    30 attempts × 50 ms = 1.5 s is calibrated to ride out the worst-case
    visibility window seen in CI without hiding real failures behind a
    long retry budget.
    """
    last_exc: Optional[Exception] = None
    for _ in range(attempts):
        try:
            await conn.ft(physical).aliasupdate(alias)
            return
        except redis.ResponseError as exc:
            last_exc = exc
            # Only retry the transient visibility race. Other errors
            # (unknown command, alias already pointing elsewhere, etc.)
            # are real and should propagate.
            if "SEARCH_INDEX_NOT_FOUND" not in str(exc):
                raise
            await asyncio.sleep(0.05)
    # Exhausted retries; surface the last error.
    assert last_exc is not None
    raise last_exc


async def create_index(
    conn: Union[redis.Redis, redis.RedisCluster], index_name, schema, current_hash
):
    if isinstance(conn, redis.RedisCluster):
        await _create_index_cluster(conn, index_name, schema, current_hash)
        await _wait_for_index(conn, index_name)
        return

    db_number = conn.connection_pool.connection_kwargs.get("db")
    if db_number and db_number > 0:
        raise MigrationError(
            "Creating search indexes is only supported in database 0. "
            f"You attempted to create an index in database {db_number}"
        )
    try:
        await conn.ft(index_name).info()
    except redis.ResponseError:
        try:
            await conn.execute_command("FT.CREATE", index_name, *schema.split())
        except redis.ResponseError as exc:
            # Race: another process created the index between our
            # existence check and FT.CREATE. Treat that as success, but
            # re-raise any other error so it isn't silently swallowed.
            if "Index already exists" not in str(exc):
                raise
        await conn.set(schema_hash_key(index_name), current_hash)
        await _wait_for_index(conn, index_name)
    else:
        log.info("Index already exists, skipping. Index hash: %s", index_name)


# ── Alias helpers ───────────────────────────────────────────────────


async def _resolve_alias_or_index(conn, name: str) -> tuple[bool, Optional[str]]:
    """Distinguish an alias from a physical index.

    Returns ``(is_alias, resolved_physical_name)``:

      * ``(True, X)``  — ``name`` is an alias pointing at physical index ``X``
      * ``(False, X)`` — ``name`` is itself a physical index named ``X``
                        (``X == name`` in this case)
      * ``(False, None)`` — neither an alias nor an index exists under ``name``

    ``FT.INFO`` on an alias returns the underlying index's info, where
    ``index_name`` is the physical index's real name (different from the
    alias). ``FT.INFO`` on a physical index returns ``index_name == name``.
    We use that distinction to tell them apart.
    """
    try:
        info = await conn.ft(name).info()
    except redis.ResponseError:
        return (False, None)
    underlying = info.get("index_name")
    if isinstance(underlying, bytes):
        underlying = underlying.decode("utf-8")
    if underlying is None:
        # Defensive: FT.INFO should always include index_name.
        return (False, None)
    # If the reported index_name differs from what we asked about, then
    # ``name`` is an alias resolving to ``underlying``. Otherwise ``name``
    # is itself a physical index.
    return (underlying != name, underlying)


async def _list_indexes(conn) -> List[str]:
    """Return all FT index names via ``FT._LIST``.

    Works for both standalone and cluster connections.
    """
    try:
        result = await conn.execute_command("FT._LIST")
    except redis.ResponseError:
        return []
    # Normalize bytes and RESP3 nesting variations.
    names: List[str] = []
    for item in result:
        if isinstance(item, bytes):
            names.append(item.decode("utf-8"))
        elif isinstance(item, str):
            names.append(item)
        elif isinstance(item, (list, tuple)) and item:
            # Some RESP3 shapes nest the name as the first element.
            first = item[0]
            if isinstance(first, bytes):
                names.append(first.decode("utf-8"))
            elif isinstance(first, str):
                names.append(first)
    return names


async def _create_physical_index_cluster(conn, index_name, schema):
    """Create a physical index on a cluster, tolerating races."""
    try:
        await conn.ft(index_name).info()
        return
    except redis.ResponseError:
        pass
    command = f"ft.create {index_name} {schema}".split()
    try:
        await conn.execute_command(*command, target_nodes=redis.RedisCluster.RANDOM)
    except redis.ResponseError as exc:
        if "Index already exists" not in str(exc):
            raise


async def create_physical_index(conn, index_name, schema):
    """Create a physical index if it does not yet exist.

    Differs from ``create_index`` in that it does NOT maintain the legacy
    ``{index_name}:hash`` bookkeeping key, which is irrelevant for
    alias-managed indexes (the version is encoded in the name itself).
    """
    if isinstance(conn, redis.RedisCluster):
        await _create_physical_index_cluster(conn, index_name, schema)
    else:
        db_number = conn.connection_pool.connection_kwargs.get("db")
        if db_number and db_number > 0:
            raise MigrationError(
                "Creating search indexes is only supported in database 0. "
                f"You attempted to create an index in database {db_number}"
            )
        try:
            await conn.ft(index_name).info()
        except redis.ResponseError:
            try:
                await conn.execute_command("FT.CREATE", index_name, *schema.split())
            except redis.ResponseError as exc:
                if "Index already exists" not in str(exc):
                    raise
    await _wait_for_index(conn, index_name)


class MigrationAction(Enum):
    CREATE = 2
    DROP = 1
    # Alias-based actions
    ALIAS_CREATE_INDEX = 3  # create the versioned physical index only
    ALIAS_LINK = 4  # point the alias at the physical index (fresh install)
    ALIAS_ADOPT = 5  # drop legacy physical index + alias the new one
    ALIAS_SWAP = 6  # repoint an existing alias atomically
    ALIAS_CLEANUP = 7  # drop stale sibling physical indexes (no docs)


@dataclass
class IndexMigration:
    model_name: str
    index_name: str
    schema: str
    hash: str
    action: MigrationAction
    conn: Union[redis.Redis, redis.RedisCluster]
    previous_hash: Optional[str] = None
    # Alias-mode bookkeeping
    alias_name: Optional[str] = None
    # Indexes that should be dropped after a successful alias swap. Each
    # entry is a stale physical index name left over from a previous
    # schema version. They are dropped WITHOUT deleting documents so the
    # underlying JSON/hash keys survive — the new physical index already
    # references them.
    stale_physical_indexes: List[str] = field(default_factory=list)

    async def run(self):
        if self.action is MigrationAction.CREATE:
            await self.create()
        elif self.action is MigrationAction.DROP:
            await self.drop()
        elif self.action is MigrationAction.ALIAS_CREATE_INDEX:
            await self._alias_create_index()
        elif self.action is MigrationAction.ALIAS_LINK:
            await self._alias_link()
        elif self.action is MigrationAction.ALIAS_ADOPT:
            await self._alias_adopt()
        elif self.action is MigrationAction.ALIAS_SWAP:
            await self._alias_swap()
        elif self.action is MigrationAction.ALIAS_CLEANUP:
            await self._alias_cleanup()

    async def create(self):
        try:
            await create_index(self.conn, self.index_name, self.schema, self.hash)
        except redis.ResponseError as exc:
            # ``create_index`` already handles the common "another worker
            # beat us to it" race, but defensively tolerate it here too so
            # a concurrent migrator doesn't surface a noisy error.
            if "Index already exists" not in str(exc):
                raise
            log.info("Index already exists: %s", self.index_name)

    async def drop(self):
        # ``delete_documents=False`` (the redis-py default) is critical here:
        # dropping the index must NOT delete the underlying JSON / hash keys.
        # The new index that the companion CREATE action builds will re-index
        # those same keys (they share the same PREFIX). Passing ``True`` here
        # was a regression that silently wiped every document on every schema
        # change — see tests/test_migrator_alias.py::test_legacy_mode_preserves_documents.
        try:
            await self.conn.ft(self.index_name).dropindex(delete_documents=False)
        except redis.ResponseError:
            log.info("Index does not exist: %s", self.index_name)

    # ── Alias-mode action implementations ───────────────────────────

    async def _alias_create_index(self):
        """Create the versioned physical index if it does not yet exist.

        This action does NOT touch the alias. A subsequent ``ALIAS_LINK``
        (fresh install), ``ALIAS_ADOPT`` (legacy adoption), or ``ALIAS_SWAP``
        (forward migration) action is responsible for pointing the alias
        at this physical index.
        """
        physical = self.index_name  # already the versioned name
        await create_physical_index(self.conn, physical, self.schema)

    async def _alias_link(self):
        """Point the alias at a freshly-created physical index.

        Used for the fresh-install path where no legacy index or alias
        existed, so there is no namespace collision. The companion
        ``ALIAS_CREATE_INDEX`` action has already built the physical index.
        """
        alias = self.alias_name
        physical = self.index_name
        if not alias or alias == physical:
            return
        try:
            # ALIASUPDATE is idempotent: if a sibling worker already linked
            # the alias to the same physical index, this is a no-op.
            # Retried to handle transient visibility races where the
            # freshly-created physical isn't immediately resolvable.
            await _retry_aliasupdate(self.conn, physical, alias)
        except redis.ResponseError as exc:
            log.warning(
                "Failed to point alias %s at physical index %s: %s",
                alias,
                physical,
                exc,
            )
            raise

    async def _alias_adopt(self):
        """Adopt an existing legacy physical index into the alias model.

        Before this runs, ``_alias_create_index`` will have built the new
        versioned physical index (sharing the same PREFIX, so RediSearch
        has re-indexed every existing document into it). This step:

          1. Drops the legacy physical index (named exactly ``alias_name``)
             WITHOUT deleting documents.
          2. Creates the alias pointing at the new versioned physical index.

        The brief window between (1) and (2) means queries to ``alias_name``
        fail momentarily. This is a one-time adoption cost when first
        switching on ``zero_downtime_migrations`` against a Redis that already
        contains a legacy index. Subsequent migrations never take this path
        again.
        """
        alias = self.alias_name
        physical = self.index_name
        if not alias:
            return

        # Drop the legacy physical index WITHOUT deleting documents.
        # ``delete_documents=False`` is critical here — the documents are
        # what the new physical index needs to keep indexing.
        try:
            await self.conn.ft(alias).dropindex(delete_documents=False)
        except redis.ResponseError:
            # The legacy index may already have been dropped by a sibling
            # worker that ran the adoption a moment earlier. That's fine.
            log.info(
                "Legacy index %s not present during adoption (likely already adopted): %s",
                alias,
                "",
            )

        # Now that the alias name is free, point it at the new physical index.
        try:
            # Retried for the same transient-visibility reasons as
            # ``_alias_link``; the physical was created moments ago and
            # may not be fully resolvable yet.
            await _retry_aliasupdate(self.conn, physical, alias)
        except redis.ResponseError as exc:
            log.warning(
                "Failed to point alias %s at physical index %s during adoption: %s",
                alias,
                physical,
                exc,
            )
            raise

    async def _alias_swap(self):
        """Atomically repoint an existing alias at a new physical index.

        Pre-condition: the new physical index has already been created and
        fully indexed (done by a preceding ``ALIAS_CREATE_INDEX`` action).
        This step is the actual zero-downtime cutover.
        """
        alias = self.alias_name
        physical = self.index_name
        if not alias:
            return

        try:
            # Retried for transient visibility races against the
            # companion CREATE_INDEX action.
            await _retry_aliasupdate(self.conn, physical, alias)
        except redis.ResponseError as exc:
            # A sibling worker may have already swapped. If so, the alias
            # now points at our physical index (or a newer one); treat
            # both as success.
            if "does not exist" not in str(exc).lower():
                raise

    async def _alias_cleanup(self):
        """Drop stale physical indexes from previous schema versions.

        Crucially we pass ``delete_documents=False``. The underlying
        JSON/hash keys are still referenced by the now-active physical
        index (and possibly by application code reading them directly via
        ``JSON.GET`` / ``HGETALL``). Deleting documents here would be the
        same catastrophic data-loss bug the legacy DROP path has.
        """
        for stale in self.stale_physical_indexes:
            if not stale:
                continue
            try:
                await self.conn.ft(stale).dropindex(delete_documents=False)
            except redis.ResponseError:
                # Already cleaned up by a sibling, or never existed.
                log.info("Stale physical index %s not present during cleanup", stale)

    def summary(self) -> str:
        """Return a short human-readable description of this migration."""
        parts = [f"{self.action.name} {self.model_name}"]
        if self.alias_name and self.alias_name != self.index_name:
            parts.append(f"alias={self.alias_name}")
        parts.append(f"index={self.index_name}")
        if self.previous_hash is not None:
            parts.append(f"from={self.previous_hash}")
        parts.append(f"to={self.hash}")
        if self.stale_physical_indexes:
            parts.append(f"cleanup={self.stale_physical_indexes}")
        return " ".join(parts)

    def history_record(self) -> Dict[str, Any]:
        """Return a JSON-serialisable record describing this migration."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "model": self.model_name,
            "index": self.index_name,
            "alias": self.alias_name,
            "action": self.action.name,
            "hash": self.hash,
            "previous_hash": self.previous_hash,
            "stale_physical_indexes": list(self.stale_physical_indexes),
        }


class Migrator:
    def __init__(
        self,
        module=None,
        conn: Optional[Union[redis.Redis, redis.RedisCluster]] = None,
        history_key: Optional[str] = None,
        allow_forward_swap: bool = False,
    ):
        self.module = module
        self.conn = conn
        # Per-instance override for the migration history list. Defaults to
        # the module-level ``MIGRATION_HISTORY_KEY`` constant so existing
        # callers keep working unchanged.
        self.history_key = history_key or MIGRATION_HISTORY_KEY
        self.migrations: List[IndexMigration] = []
        # When ``False`` (the default), the migrator is conservative and
        # safe to invoke on every app boot even during a rolling deploy:
        # it will create indexes for fresh installs, adopt legacy indexes,
        # and clean up stale physical indexes, but it will NOT swap an
        # existing alias to a newly-created physical index.
        #
        # Set to ``True`` for an explicit pre-deploy migration step where
        # only one version of the application is running. This allows the
        # forward swap needed to migrate schema changes. See the docs for
        # the full rolling-deploy guidance.
        self.allow_forward_swap = allow_forward_swap

    async def detect_migrations(self):
        self.migrations = []
        # Try to load any modules found under the given path or module name.
        if self.module:
            import_submodules(self.module)

        # Import this at run-time to avoid triggering import-time side effects,
        # e.g. checks for RedisJSON, etc.
        from aredis_om.model.model import _model_registry_lock, model_registry

        # When no connection is given (bare ``Migrator().run()``), skip models
        # marked ``_test_only = True``. This prevents module-level test model
        # classes that leaked into the global registry from being picked up by
        # other tests' bare migrator calls on the same xdist worker.
        #
        # When a connection IS provided (``Migrator(conn=redis).run()``),
        # _test_only is ignored so that explicit migrator runs (e.g. in
        # migration-specific tests) still process the test models normally.
        # In that mode, isolation between tests is the responsibility of the
        # test fixtures (``_isolate_registry`` in the test files) which clear
        # all model_registry entries that belong to other tests before
        # each migration runs.
        skip_test_only = self.conn is None

        # Snapshot the registry under the lock to avoid
        # ``dict changed size during iteration`` if another thread defines a
        # new model class while we're iterating.
        with _model_registry_lock:
            entries = list(model_registry.items())

        for name, cls in entries:
            if skip_test_only and getattr(cls.Meta, "_test_only", False):
                continue
            use_alias = bool(getattr(cls.Meta, "zero_downtime_migrations", False))
            if use_alias:
                await self._detect_alias_migrations(name, cls)
            else:
                await self._detect_legacy_migrations(name, cls)

    async def _detect_legacy_migrations(self, name, cls):
        """Original DROP+CREATE migration path.

        Drops the index with ``delete_documents=False`` (the redis-py
        default) so the underlying JSON / hash keys survive and are re-indexed
        by the immediately-following CREATE. This is data-safe for additive
        schema changes (adding indexed fields) but has a brief query-gap
        window while the index is being rebuilt. For zero-downtime swaps with
        no query gap, opt in via ``Meta.zero_downtime_migrations = True``.
        """
        conn = self.conn or cls.db()
        try:
            schema = cls.redisearch_schema()
        except NotImplementedError:
            log.info("Skipping migrations for %s", name)
            return
        current_hash = hashlib.sha1(schema.encode("utf-8")).hexdigest()  # nosec
        hash_key = schema_hash_key(cls.Meta.index_name)

        try:
            await conn.ft(cls.Meta.index_name).info()
        except redis.ResponseError:
            self.migrations.append(
                IndexMigration(
                    name,
                    cls.Meta.index_name,
                    schema,
                    current_hash,
                    MigrationAction.CREATE,
                    conn,
                )
            )
            return

        stored_hash = await conn.get(hash_key)
        if isinstance(stored_hash, bytes):
            stored_hash = stored_hash.decode("utf-8")

        schema_out_of_date = current_hash != stored_hash

        if schema_out_of_date:
            # Drop and recreate the index in place. ``drop()`` uses
            # ``delete_documents=False`` so the underlying JSON / hash keys
            # survive and are re-indexed by the new index (both share the
            # same PREFIX). This is data-safe but there IS a brief query-gap
            # window while the index is being rebuilt. For zero-downtime
            # swaps with no query gap, set ``Meta.zero_downtime_migrations =
            # True`` on the model to use the FT.ALIASUPDATE path.
            self.migrations.append(
                IndexMigration(
                    name,
                    cls.Meta.index_name,
                    schema,
                    current_hash,
                    MigrationAction.DROP,
                    conn,
                    stored_hash,
                )
            )
            self.migrations.append(
                IndexMigration(
                    name,
                    cls.Meta.index_name,
                    schema,
                    current_hash,
                    MigrationAction.CREATE,
                    conn,
                    stored_hash,
                )
            )

    async def _detect_alias_migrations(self, name, cls):
        """Zero-downtime migration path using FT.ALIASUPDATE.

        Physical indexes are named ``{alias}__v{hash8}`` where the alias is
        ``Meta.index_name`` and the hash is the first 8 hex chars of the
        SHA1 of the schema string. The alias is repointed atomically when
        the schema changes; underlying documents are never deleted.

        See the project docs for the full concurrency model. The key rules
        that make this safe under rolling deploys:

          * Adding indexed fields is always safe (the new physical index is
            a superset of the old).
          * The migrator only swaps FORWARD: if the alias already points to
            a different physical index and our target already exists, we
            assume a newer migration has won and do nothing.
          * All primitive operations (CREATE / ALIASUPDATE / DROP) are
            idempotent so concurrent migrators are safe.
        """
        conn = self.conn or cls.db()
        try:
            schema = cls.redisearch_schema()
        except NotImplementedError:
            log.info("Skipping migrations for %s", name)
            return

        alias = cls.Meta.index_name
        current_hash = hashlib.sha1(schema.encode("utf-8")).hexdigest()  # nosec
        target_physical = physical_index_name(alias, current_hash)

        # 1. Probe the name to find out whether it's an alias, a physical
        #    index, or nothing at all.
        is_alias, resolved = await _resolve_alias_or_index(conn, alias)

        if resolved is None:
            # Neither an alias nor a physical index exists under ``alias``.
            # This is a fresh install: create the versioned physical index
            # and alias it.
            self.migrations.append(
                IndexMigration(
                    name,
                    target_physical,
                    schema,
                    current_hash,
                    MigrationAction.ALIAS_CREATE_INDEX,
                    conn,
                    alias_name=alias,
                )
            )
            self.migrations.append(
                IndexMigration(
                    name,
                    target_physical,
                    schema,
                    current_hash,
                    MigrationAction.ALIAS_LINK,
                    conn,
                    alias_name=alias,
                )
            )
            return

        if not is_alias:
            # ``alias`` is itself a physical index — a legacy install from
            # before alias mode was enabled. Build the new versioned
            # physical index (auto-indexes existing documents because it
            # shares the PREFIX), drop the legacy one WITHOUT deleting
            # documents, then point the alias at the new one.
            self.migrations.append(
                IndexMigration(
                    name,
                    target_physical,
                    schema,
                    current_hash,
                    MigrationAction.ALIAS_CREATE_INDEX,
                    conn,
                    alias_name=alias,
                )
            )
            self.migrations.append(
                IndexMigration(
                    name,
                    target_physical,
                    schema,
                    current_hash,
                    MigrationAction.ALIAS_ADOPT,
                    conn,
                    alias_name=alias,
                )
            )
            return

        # 2. Alias exists and points at ``resolved``.
        if resolved == target_physical:
            # Idempotent: alias already resolves to our schema version.
            # Optionally clean up any stale sibling physical indexes.
            stale = await self._find_stale_physical_indexes(
                conn, alias, target_physical
            )
            if stale:
                self.migrations.append(
                    IndexMigration(
                        name,
                        target_physical,
                        schema,
                        current_hash,
                        MigrationAction.ALIAS_CLEANUP,
                        conn,
                        alias_name=alias,
                        stale_physical_indexes=stale,
                    )
                )
            return

        # 3. Alias points at a DIFFERENT physical index. Two sub-cases:
        #
        #   (a) target_physical does not exist yet → would need create+swap.
        #       This is forward migration in a sequential deploy, but it is
        #       also the "swap back" pattern in a rolling deploy (old code
        #       booting after new code). Without deploy-level version
        #       ordering we cannot tell them apart, so we gate this on
        #       ``allow_forward_swap``.
        #
        #   (b) target_physical already exists but alias points elsewhere →
        #       a newer migration already won. Never swap back.
        all_indexes = await _list_indexes(conn)
        target_exists = target_physical in all_indexes

        if not target_exists and not self.allow_forward_swap:
            log.error(
                "Alias %s points at %s but target %s does not exist. "
                "Refusing to create+swap because allow_forward_swap=False "
                "(rolling-deploy safety). Run the migrator with "
                "allow_forward_swap=True as a pre-deploy step to apply "
                "this schema change.",
                alias,
                resolved,
                target_physical,
            )
            return

        if target_exists:
            # target exists but alias points elsewhere → newer migration won.
            log.warning(
                "Alias %s points at %s but target %s already exists; a "
                "newer migration has already run. Not swapping back "
                "(rolling-deploy safety).",
                alias,
                resolved,
                target_physical,
            )
            return

        # Forward migration (only reached when allow_forward_swap=True).
        self.migrations.append(
            IndexMigration(
                name,
                target_physical,
                schema,
                current_hash,
                MigrationAction.ALIAS_CREATE_INDEX,
                conn,
                alias_name=alias,
                previous_hash=resolved,
            )
        )
        self.migrations.append(
            IndexMigration(
                name,
                target_physical,
                schema,
                current_hash,
                MigrationAction.ALIAS_SWAP,
                conn,
                alias_name=alias,
                previous_hash=resolved,
            )
        )
        # After the swap, drop stale sibling physical indexes (no docs).
        stale = await self._find_stale_physical_indexes(conn, alias, target_physical)
        if stale:
            self.migrations.append(
                IndexMigration(
                    name,
                    target_physical,
                    schema,
                    current_hash,
                    MigrationAction.ALIAS_CLEANUP,
                    conn,
                    alias_name=alias,
                    previous_hash=resolved,
                    stale_physical_indexes=stale,
                )
            )

    async def _find_stale_physical_indexes(
        self, conn, alias: str, current_physical: str
    ) -> List[str]:
        """Return versioned physical indexes for this alias that aren't current.

        Matches names like ``{alias}__v{8 hex chars}`` that are no longer
        the active physical index. These are safe to drop without deleting
        documents because the current physical index already references
        the underlying keys.
        """
        all_indexes = await _list_indexes(conn)
        prefix = f"{alias}__v"
        stale = []
        for idx in all_indexes:
            if idx == current_physical:
                continue
            if (
                idx.startswith(prefix)
                and len(idx) == len(prefix) + PHYSICAL_INDEX_HASH_LEN
            ):
                stale.append(idx)
        return stale

    async def run(self, dry_run: bool = False, record_history: bool = True):
        """Execute detected migrations.

        Args:
            dry_run: When ``True``, print the planned migrations and return
                without applying any changes.
            record_history: When ``True`` (default), append a JSON record per
                applied migration to the Redis list keyed by
                ``MIGRATION_HISTORY_KEY``. Failures to record history are
                logged and never abort the migration itself.
        """
        if not self.migrations:
            await self.detect_migrations()

        if dry_run:
            if not self.migrations:
                print("No pending migrations.")
                return
            print(f"Dry run: {len(self.migrations)} migration(s) planned:")
            for migration in self.migrations:
                print(f"  - {migration.summary()}")
            return

        for migration in self.migrations:
            await migration.run()
            if record_history:
                await self._record_history(migration)

    async def _record_history(self, migration: IndexMigration) -> None:
        """Best-effort append of a migration record to Redis history."""
        try:
            payload = json.dumps(migration.history_record())
            await migration.conn.rpush(self.history_key, payload)
        except Exception:  # pragma: no cover - history is best effort
            log.warning(
                "Failed to record migration history for %s",
                migration.index_name,
                exc_info=True,
            )
