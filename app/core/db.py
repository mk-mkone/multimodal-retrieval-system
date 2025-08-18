from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence

import psycopg
from psycopg import Connection, Cursor
from psycopg.rows import dict_row

from app.core.config import settings


class PostgresClient:
    """
    PostgreSQL client.

    - Connection factory with dict_row for convenient results
    - Helpers for execute/executemany/fetchone/fetchall
    - Transaction context manager
    """

    def __init__(self, dsn: str | None = None, autocommit: bool = True) -> None:
        self._dsn = dsn or str(settings.POSTGRES_URI)
        self._autocommit = autocommit

    def connect(self) -> Connection:
        return psycopg.connect(
            self._dsn, autocommit=self._autocommit, row_factory=dict_row
        )

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """
        Open a connection with autocommit disabled and yield it within a transaction.
        Commits on successful exit, rolls back on exception.
        """
        conn = psycopg.connect(self._dsn, autocommit=False, row_factory=dict_row)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]) -> None:
        with self.connection() as conn, conn.cursor() as cur:
            cur.executemany(sql, seq_of_params)

    def fetchone(self, sql: str, params: Sequence[Any] | None = None) -> dict | None:
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()

    def fetchall(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
