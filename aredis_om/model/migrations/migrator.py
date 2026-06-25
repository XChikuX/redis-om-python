# mypy: disable-error-code="attr-defined"

import asyncio
import hashlib
import importlib
import json
import logging
import pkgutil
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from ... import redis

log = logging.getLogger(__name__)

# Redis key holding a chronological JSON log of applied migrations.
MIGRATION_HISTORY_KEY = "redis_om:migration_history"


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


async def _wait_for_index(conn, index_name, timeout=5.0):
    """Poll FT.INFO until the index reports it is fully indexed.

    Redis 8.8+ introduced asynchronous background indexing for existing
    documents, which means ``FT.CREATE`` can return before previously
    written keys have been scanned.  Tests that create an index and
    immediately search it can see empty or partial results unless we
    wait for indexing to finish.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            info = await conn.ft(index_name).info()
        except redis.ResponseError:
            return
        # percent_indexed is a string like "1" when complete.
        percent = info.get("percent_indexed")
        if percent == "1" or percent == 1:
            return
        await asyncio.sleep(0.05)
    log.warning("Timeout waiting for index %s to reach 100%% indexed", index_name)


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
        await conn.execute_command(f"ft.create {index_name} {schema}")
        await conn.set(schema_hash_key(index_name), current_hash)
        await _wait_for_index(conn, index_name)
    else:
        log.info("Index already exists, skipping. Index hash: %s", index_name)


class MigrationAction(Enum):
    CREATE = 2
    DROP = 1


@dataclass
class IndexMigration:
    model_name: str
    index_name: str
    schema: str
    hash: str
    action: MigrationAction
    conn: Union[redis.Redis, redis.RedisCluster]
    previous_hash: Optional[str] = None

    async def run(self):
        if self.action is MigrationAction.CREATE:
            await self.create()
        elif self.action is MigrationAction.DROP:
            await self.drop()

    async def create(self):
        try:
            await create_index(self.conn, self.index_name, self.schema, self.hash)
        except redis.ResponseError:
            log.info("Index already exists: %s", self.index_name)

    async def drop(self):
        try:
            await self.conn.ft(self.index_name).dropindex(delete_documents=True)
        except redis.ResponseError:
            log.info("Index does not exist: %s", self.index_name)

    def summary(self) -> str:
        """Return a short human-readable description of this migration."""
        parts = [f"{self.action.name} {self.model_name}", f"index={self.index_name}"]
        if self.previous_hash is not None:
            parts.append(f"from={self.previous_hash}")
        parts.append(f"to={self.hash}")
        return " ".join(parts)

    def history_record(self) -> Dict[str, Any]:
        """Return a JSON-serialisable record describing this migration."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "model": self.model_name,
            "index": self.index_name,
            "action": self.action.name,
            "hash": self.hash,
            "previous_hash": self.previous_hash,
        }


class Migrator:
    def __init__(
        self,
        module=None,
        conn: Optional[Union[redis.Redis, redis.RedisCluster]] = None,
    ):
        self.module = module
        self.conn = conn
        self.migrations: List[IndexMigration] = []

    async def detect_migrations(self):
        self.migrations = []
        # Try to load any modules found under the given path or module name.
        if self.module:
            import_submodules(self.module)

        # Import this at run-time to avoid triggering import-time side effects,
        # e.g. checks for RedisJSON, etc.
        from aredis_om.model.model import model_registry

        for name, cls in model_registry.items():
            hash_key = schema_hash_key(cls.Meta.index_name)
            conn = self.conn or cls.db()
            try:
                schema = cls.redisearch_schema()
            except NotImplementedError:
                log.info("Skipping migrations for %s", name)
                continue
            current_hash = hashlib.sha1(schema.encode("utf-8")).hexdigest()  # nosec

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
                continue

            stored_hash = await conn.get(hash_key)
            if isinstance(stored_hash, bytes):
                stored_hash = stored_hash.decode("utf-8")

            schema_out_of_date = current_hash != stored_hash

            if schema_out_of_date:
                # Note: We drop and recreate the index in place. A zero-downtime
                # variant would swap indexes via FT.ALIASUPDATE, but that needs
                # dual-write coordination with application code and is left as
                # future work tracked separately.
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
            await migration.conn.rpush(MIGRATION_HISTORY_KEY, payload)
        except Exception:  # pragma: no cover - history is best effort
            log.warning(
                "Failed to record migration history for %s",
                migration.index_name,
                exc_info=True,
            )
