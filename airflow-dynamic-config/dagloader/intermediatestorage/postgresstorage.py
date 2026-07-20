from dagloader.intermediatestorage.storage import Storage
from airflow.providers.postgres.hooks.postgres import PostgresHook
import hashlib
import pickle
from typing import Any
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
    """

    def __init__(self, conn_id: str, database: str = None,
                 schema: str = "public",
                 table_prefix: str = "intermediate_results",
                 hypertable: bool = True):
        self.conn_id = conn_id
        # Optional override of the connection's database. Leave unset to use
        # the database configured on the Airflow connection. If set, the
        # database must already exist on the server (Postgres cannot create it
        # inside this session).
        self.database = database
        self.schema = schema
        self.table_prefix = table_prefix
        self.hypertable = hypertable
        self.namespace = ""

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

    def _ensure_table_exists(self, table_name: str) -> None:
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
        if self.hypertable:
            try:
                hook.run(
                    "SELECT create_hypertable(%s, 'created_at', "
                    "if_not_exists => TRUE)",
                    parameters=(qualified,),
                )
            except Exception as e:
                logger.warning(
                    f"Could not create hypertable for {qualified}; continuing "
                    f"with a plain Postgres table: {e}"
                )

    def save(self, key: str, data: Any) -> None:
        table_name = self._table_name_for_key(key)
        self._ensure_table_exists(table_name)
        payload = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        hook = self._hook()
        hook.run(
            f"INSERT INTO {self._qualified(table_name)} "
            f"(payload, run_id) VALUES (%s, %s)",
            parameters=(payload, self._effective_run_id()),
        )

    def load(self, key: str, scoped: bool = True) -> Any:
        table_name = self._table_name_for_key(key)
        self._ensure_table_exists(table_name)
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
        return pickle.loads(bytes(payload))

    def init(self, **kwargs):
        directory_name = kwargs.get('directory_name')
        if directory_name:
            self.namespace = self._sanitize_identifier(directory_name)
