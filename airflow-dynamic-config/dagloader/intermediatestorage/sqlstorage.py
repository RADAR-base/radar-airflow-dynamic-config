from dagloader.intermediatestorage.storage import Storage
import os
import pickle
import sqlite3
from typing import Any
import logging
logger = logging.getLogger(__name__)


class SQLStorage(Storage):
    def __init__(self, conn_id: str, database: str,
                 table_prefix: str = "intermediate_results"):
        self.conn_id = conn_id
        self.database = database
        self.table_prefix = table_prefix
        self.db_path = database
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

    def _table_name_for_key(self, key: str) -> str:
        base = self._sanitize_identifier(self.table_prefix)
        key_part = self._sanitize_identifier(key)
        if self.namespace:
            return f"{base}_{self.namespace}_{key_part}"
        return f"{base}_{key_part}"

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _ensure_table_exists(self, table_name: str) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload BLOB NOT NULL
                )
                """
            )
            conn.commit()

    def save(self, key: str, data: Any) -> None:
        table_name = self._table_name_for_key(key)
        self._ensure_table_exists(table_name)
        payload = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {table_name} (payload)
                VALUES (?)
                """,
                (payload,)
            )
            conn.commit()

    def load(self, key: str) -> Any:
        table_name = self._table_name_for_key(key)
        self._ensure_table_exists(table_name)
        with self._connect() as conn:
            cursor = conn.execute(
                f"SELECT payload FROM {table_name} ORDER BY id DESC LIMIT 1"
            )
            row = cursor.fetchone()

        if row is None:
            raise FileNotFoundError(f"No data found for key: {key}")

        return pickle.loads(row[0])

    def init(self, **kwargs):
        directory_name = kwargs.get('directory_name')
        if directory_name:
            self.namespace = self._sanitize_identifier(directory_name)
