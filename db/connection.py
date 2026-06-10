"""
db/connection.py
================
PostgreSQL connection management using psycopg2's ThreadedConnectionPool.

Provides a context-manager-based pool so every caller automatically
returns connections even on exception. The pool is a module-level
singleton — call `get_pool()` everywhere; it is created once.
"""
import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor

from config.settings import DatabaseSettings

logger = logging.getLogger(__name__)

_pool: pg_pool.ThreadedConnectionPool | None = None


def get_pool() -> pg_pool.ThreadedConnectionPool:
    """Return the module-level connection pool, creating it on first call."""
    global _pool
    if _pool is None or _pool.closed:
        params = DatabaseSettings.psycopg2_params()
        _pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            **params,
        )
        logger.info(
            "PostgreSQL pool created: %s@%s:%s/%s",
            params["user"], params["host"], params["port"], params["dbname"],
        )
    return _pool


def close_pool() -> None:
    """Close all connections in the pool. Call at application shutdown."""
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
        logger.info("PostgreSQL pool closed.")


class DatabasePool:
    """
    Thin wrapper around the psycopg2 pool that exposes a clean
    context-manager API used by the loader and transformer.

    Usage:
        pool = DatabasePool()
        with pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """

    def __init__(self):
        self._pool = get_pool()

    @contextmanager
    def get_connection(self) -> Generator:
        """Yield a connection from the pool, returning it on exit."""
        conn = self._pool.getconn()
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    @contextmanager
    def cursor(self, dict_cursor: bool = False) -> Generator:
        """Convenience: yield a cursor directly, auto-committing on success."""
        factory = RealDictCursor if dict_cursor else None
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=factory) as cur:
                yield cur
            conn.commit()

    def execute(self, sql: str, params=None) -> None:
        """Execute a single statement with auto-commit."""
        with self.cursor() as cur:
            cur.execute(sql, params)

    def fetchall(self, sql: str, params=None) -> list[dict]:
        """Execute a SELECT and return all rows as a list of dicts."""
        with self.cursor(dict_cursor=True) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def test_connection(self) -> bool:
        """Verify the database is reachable. Returns True on success."""
        try:
            with self.cursor() as cur:
                cur.execute("SELECT 1")
            logger.info("Database connection OK.")
            return True
        except Exception as exc:
            logger.error("Database connection FAILED: %s", exc)
            return False
