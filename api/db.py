"""PostgreSQL connection pool (psycopg 3)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from config import postgres_config

_pool: ConnectionPool | None = None


def open_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            postgres_config().dsn + " options='-c timezone=UTC'",
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row, "autocommit": True},
            open=False,
        )
        _pool.open(wait=False)
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def connection() -> Iterator:
    with open_pool().connection(timeout=10) as conn:
        yield conn


def fetch_all(sql: str, params: dict | tuple | None = None) -> list[dict]:
    with connection() as conn:
        return conn.execute(sql, params).fetchall()


def fetch_one(sql: str, params: dict | tuple | None = None) -> dict | None:
    with connection() as conn:
        return conn.execute(sql, params).fetchone()
