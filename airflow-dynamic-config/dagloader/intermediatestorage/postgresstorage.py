from dagloader.intermediatestorage.storage import Storage
from airflow.providers.postgres.hooks.postgres import PostgresHook
import hashlib
import pickle
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)

# Postgres truncates identifiers at NAMEDATALEN-1 bytes *silently*, so two
# long names sharing a prefix end up pointing at the same table. Anything
# longer is hashed down instead (see _fit_identifier).
MAX_IDENTIFIER_LENGTH = 63
_HASH_LENGTH = 8


class PostgresStorage(Storage):
    """Intermediate-results storage backed by PostgreSQL / TimescaleDB.

    Each ``save()`` appends one row holding a pickled snapshot of the data for
    a key; ``load()`` returns the most recent snapshot. Connectivity comes from
    the Airflow connection identified by ``conn_id`` (a Postgres/Timescale
    connection). When the TimescaleDB extension is available the backing table
    is converted to a hypertable partitioned on ``created_at``; on plain
    Postgres it gracefully stays a regular table.

    ``retention_time`` (days, may be fractional) bounds how long snapshots are
    kept; ``None`` keeps them forever. Hypertables get a native TimescaleDB
    retention policy. Plain tables (or when the policy cannot be added, e.g.
    on the Apache-licensed TimescaleDB build) are pruned on every ``save()``.
    Keys registered via ``set_retention_exempt`` (e.g. action backoff state
    with ``apply_retention_policy: false``) are never expired.
    """

    def __init__(self, conn_id: str, database: str = None,
                 schema: str = "public",
                 table_prefix: str = "intermediate_results",
                 hypertable: bool = True,
                 retention_time: Optional[float] = None):
        self.conn_id = conn_id
        # Optional override of the connection's database. Leave unset to use
        # the database configured on the Airflow connection. If set, the
        # database must already exist on the server (Postgres cannot create it
        # inside this session).
        self.database = database
        self.schema = schema
        self.table_prefix = table_prefix
        self.hypertable = hypertable
        self.retention_time = self._validate_retention_time(retention_time)
        self.namespace = ""

    @staticmethod
    def _validate_retention_time(value) -> Optional[float]:
        # Fail loudly at DAG parse time: silently ignoring a bad value would
        # mean data is never cleaned up.
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) \
                or value <= 0:
            raise ValueError(
                f"'retention_time' must be a positive number of days or "
                f"null, got {value!r}"
            )
        return float(value)

    def _retention_for_key(self, key: str) -> Optional[float]:
        if key in self.retention_exempt_keys:
            return None
        return self.retention_time

    def _sanitize_identifier(self, value: str) -> str:
        sanitized = ''.join(
            ch if ch.isalnum() or ch == '_' else '_'
            for ch in value
        ).strip('_')
        if not sanitized:
            raise ValueError("Identifier cannot be empty after sanitization")
        if sanitized[0].isdigit():
            sanitized = f"t_{sanitized}"
        return sanitized

    def _fit_identifier(self, value: str) -> str:
        """Shorten an identifier to something Postgres will not truncate.

        Truncating on its own is not safe: two keys sharing the first 63
        bytes would resolve to the same table and read each other's
        snapshots. Keeping a hash of the full name in the suffix makes the
        result unique and stable across runs.
        """
        encoded = value.encode('utf-8')
        if len(encoded) <= MAX_IDENTIFIER_LENGTH:
            return value
        digest = hashlib.sha1(
            encoded, usedforsecurity=False
        ).hexdigest()[:_HASH_LENGTH]
        keep = MAX_IDENTIFIER_LENGTH - _HASH_LENGTH - 1
        head = encoded[:keep].decode('utf-8', 'ignore').rstrip('_')
        shortened = f"{head}_{digest}"
        logger.info(
            f"Identifier '{value}' exceeds Postgres' "
            f"{MAX_IDENTIFIER_LENGTH}-byte limit; using '{shortened}'."
        )
        return shortened

    def _table_name_for_key(self, key: str) -> str:
        base = self._sanitize_identifier(self.table_prefix)
        key_part = self._sanitize_identifier(key)
        if self.namespace:
            name = f"{base}_{self.namespace}_{key_part}"
        else:
            name = f"{base}_{key_part}"
        return self._fit_identifier(name)

    def _qualified(self, table_name: str) -> str:
        return f"{self._sanitize_identifier(self.schema)}.{table_name}"

    def _hook(self):
        kwargs = {"postgres_conn_id": self.conn_id}
        if self.database:
            kwargs["database"] = self.database
        return PostgresHook(**kwargs)

    def _ensure_table_exists(self, table_name: str,
                             retention_time: Optional[float]) -> bool:
        """Create the table if needed and apply ``retention_time`` (days,
        ``None`` for no retention) to it.

        Returns True when retention is enforced by TimescaleDB itself, in
        which case ``save()`` does not need to prune rows.
        """
        hook = self._hook()
        schema = self._sanitize_identifier(self.schema)
        qualified = self._qualified(table_name)
        hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        # created_at is part of the primary key because TimescaleDB requires
        # the partitioning column to be included in any unique/primary key.
        # run_id identifies the DAG run that produced the snapshot so that
        # concurrent/overlapping runs do not read each other's data.
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified} (
                id BIGSERIAL,
                payload BYTEA NOT NULL,
                run_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (id, created_at)
            )
            """
        )
        index_name = self._fit_identifier(f"{table_name}_run_id_idx")
        hook.run(
            f"CREATE INDEX IF NOT EXISTS "
            f"{index_name} ON {qualified} (run_id)"
        )
        is_hypertable = False
        if self.hypertable:
            try:
                hook.run(
                    "SELECT create_hypertable(%s, 'created_at', "
                    "if_not_exists => TRUE)",
                    parameters=(qualified,),
                )
                is_hypertable = True
            except Exception as e:
                logger.warning(
                    f"Could not create hypertable for {qualified}; continuing "
                    f"with a plain Postgres table: {e}"
                )
        if is_hypertable and self._apply_retention_policy(
                hook, schema, table_name, qualified, retention_time):
            return True
        if retention_time is not None:
            # Hypertables already index created_at; plain tables need one so
            # the retention DELETE does not scan the whole table.
            created_idx = self._fit_identifier(f"{table_name}_created_at_idx")
            hook.run(
                f"CREATE INDEX IF NOT EXISTS "
                f"{created_idx} ON {qualified} (created_at)"
            )
        return False

    def _apply_retention_policy(self, hook, schema: str, table_name: str,
                                qualified: str,
                                retention_time: Optional[float]) -> bool:
        """Sync the hypertable's TimescaleDB retention policy with
        ``retention_time``: add it, replace it if the value changed, or drop
        it when retention is disabled. Returns False if TimescaleDB could not
        manage retention, so the caller falls back to pruning on save."""
        try:
            # Unquoted identifiers are folded to lower case in the catalog.
            row = hook.get_first(
                "SELECT (config->>'drop_after')::interval "
                "       = %s * INTERVAL '1 day' "
                "FROM timescaledb_information.jobs "
                "WHERE proc_name = 'policy_retention' "
                "  AND hypertable_schema = %s AND hypertable_name = %s",
                parameters=(retention_time or 0, schema.lower(),
                            table_name.lower()),
            )
            if row is not None and (retention_time is None or not row[0]):
                hook.run(
                    "SELECT remove_retention_policy(%s, if_exists => TRUE)",
                    parameters=(qualified,),
                )
                row = None
            if retention_time is not None and row is None:
                hook.run(
                    "SELECT add_retention_policy(%s, "
                    "drop_after => %s * INTERVAL '1 day', "
                    "if_not_exists => TRUE)",
                    parameters=(qualified, retention_time),
                )
                logger.info(
                    f"Retention policy of {retention_time} day(s) set "
                    f"on {qualified}."
                )
            return True
        except Exception as e:
            if retention_time is not None:
                logger.warning(
                    f"Could not set TimescaleDB retention policy on "
                    f"{qualified}; pruning old rows on save instead: {e}"
                )
            return False

    def _prune_expired(self, hook, table_name: str,
                       retention_time: float) -> None:
        hook.run(
            f"DELETE FROM {self._qualified(table_name)} "
            f"WHERE created_at < now() - %s * INTERVAL '1 day'",
            parameters=(retention_time,),
        )

    def save(self, key: str, data: Any) -> None:
        self.save_raw(key, pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL))

    def load(self, key: str, scoped: bool = True) -> Any:
        return pickle.loads(self.load_raw(key, scoped=scoped))

    def save_raw(self, key: str, payload: bytes) -> None:
        table_name = self._table_name_for_key(key)
        retention_time = self._retention_for_key(key)
        retention_managed = self._ensure_table_exists(table_name,
                                                      retention_time)
        hook = self._hook()
        hook.run(
            f"INSERT INTO {self._qualified(table_name)} "
            f"(payload, run_id) VALUES (%s, %s)",
            parameters=(payload, self._effective_run_id()),
        )
        if retention_time is not None and not retention_managed:
            self._prune_expired(hook, table_name, retention_time)

    def load_raw(self, key: str, scoped: bool = True) -> bytes:
        table_name = self._table_name_for_key(key)
        self._ensure_table_exists(table_name, self._retention_for_key(key))
        hook = self._hook()
        qualified = self._qualified(table_name)
        order = "ORDER BY created_at DESC, id DESC LIMIT 1"
        run_id = self._effective_run_id()
        if scoped and run_id is not None:
            row = hook.get_first(
                f"SELECT payload FROM {qualified} "
                f"WHERE run_id = %s {order}",
                parameters=(run_id,),
            )
        else:
            row = hook.get_first(f"SELECT payload FROM {qualified} {order}")
        if row is None:
            raise FileNotFoundError(f"No data found for key: {key}")
        payload = row[0]
        if isinstance(payload, memoryview):
            payload = payload.tobytes()
        return bytes(payload)

    def init(self, **kwargs):
        directory_name = kwargs.get('directory_name')
        if directory_name:
            self.namespace = self._sanitize_identifier(directory_name)
