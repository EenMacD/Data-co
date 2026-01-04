"""
Database connection management for staging and production databases.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator
from pathlib import Path

import json
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, Json
from psycopg2.extensions import register_adapter
from dotenv import load_dotenv

# Register JSON adapter for dict types only
# Note: Lists are NOT auto-converted to JSON to allow PostgreSQL array types
register_adapter(dict, Json)

# Load environment variables from project root
# This file is in Data/database/, so go up 2 levels to find .env
ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)


class DatabaseConfig:
    """Configuration for database connections."""

    def __init__(self, env_prefix: str):
        """
        Initialize database config from environment variables.

        Args:
            env_prefix: Prefix for env vars (e.g., 'STAGING' or 'PRODUCTION')
        """
        self.host = os.getenv(f"{env_prefix}_DB_HOST")
        self.port = int(os.getenv(f"{env_prefix}_DB_PORT"))
        self.database = os.getenv(f"{env_prefix}_DB_NAME")
        self.user = os.getenv(f"{env_prefix}_DB_USER")
        self.password = os.getenv(f"{env_prefix}_DB_PASSWORD")

        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError(
                f"Missing required database config for {env_prefix}. "
                f"Please set {env_prefix}_DB_NAME, {env_prefix}_DB_USER, "
                f"and {env_prefix}_DB_PASSWORD in .env"
            )

    def get_connection_string(self) -> str:
        """Get psycopg2 connection string."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


class DatabaseManager:
    """Manages database connections with connection pooling."""

    def __init__(self, config: DatabaseConfig, pool_size: int = 5):
        """
        Initialize database manager with connection pool.

        Args:
            config: Database configuration
            pool_size: Number of connections to maintain in pool
        """
        self.config = config
        self.pool = psycopg2.pool.SimpleConnectionPool(
            1,  # min connections
            pool_size,  # max connections
            config.get_connection_string(),
        )

        if not self.pool:
            raise Exception(f"Failed to create connection pool for {config.database}")

        print(f"[db] Connected to {config.database} on {config.host}:{config.port}")

    @contextmanager
    def get_connection(self) -> Generator:
        """
        Get a connection from the pool (context manager).

        Usage:
            with db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM companies")
        """
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    @contextmanager
    def get_cursor(self, dict_cursor: bool = True) -> Generator:
        """
        Get a cursor directly (context manager).

        Args:
            dict_cursor: If True, return RealDictCursor (rows as dicts)

        Usage:
            with db_manager.get_cursor() as cur:
                cur.execute("SELECT * FROM companies")
                rows = cur.fetchall()
        """
        conn = self.pool.getconn()
        try:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
        finally:
            self.pool.putconn(conn)

    def execute(
        self, query: str, params: tuple | dict | None = None, fetch: bool = False
    ) -> list | None:
        """
        Execute a query with automatic connection management.

        Args:
            query: SQL query to execute
            params: Query parameters (tuple or dict)
            fetch: Whether to fetch and return results

        Returns:
            List of rows if fetch=True, None otherwise
        """
        with self.get_cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            return None

    def execute_many(self, query: str, params_list: list[tuple | dict]) -> None:
        """
        Execute the same query with multiple parameter sets.

        Args:
            query: SQL query to execute
            params_list: List of parameter tuples/dicts
        """
        with self.get_cursor(dict_cursor=False) as cur:
            cur.executemany(query, params_list)

    def close(self) -> None:
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            print(f"[db] Closed connection pool for {self.config.database}")


# Global database managers (initialized lazily)
_staging_db: DatabaseManager | None = None
_production_db: DatabaseManager | None = None


def get_staging_db() -> DatabaseManager:
    """Get staging database manager (singleton)."""
    global _staging_db
    if _staging_db is None:
        config = DatabaseConfig("STAGING")
        _staging_db = DatabaseManager(config)
    return _staging_db


def get_production_db() -> DatabaseManager:
    """Get production database manager (singleton)."""
    global _production_db
    if _production_db is None:
        config = DatabaseConfig("PRODUCTION")
        _production_db = DatabaseManager(config)
    return _production_db


def close_all_connections() -> None:
    """Close all database connections."""
    global _staging_db, _production_db
    if _staging_db:
        _staging_db.close()
        _staging_db = None
    if _production_db:
        _production_db.close()
        _production_db = None


# Convenience functions
def staging_execute(query: str, params=None, fetch: bool = False):
    """Execute query on staging database."""
    return get_staging_db().execute(query, params, fetch)


def production_execute(query: str, params=None, fetch: bool = False):
    """Execute query on production database."""
    return get_production_db().execute(query, params, fetch)


if __name__ == "__main__":
    # Test connection
    print("Testing database connections...")

    try:
        staging = get_staging_db()
        print("✓ Staging database connected")

        # Test query
        result = staging.execute("SELECT version();", fetch=True)
        print(f"  PostgreSQL version: {result[0]['version'][:50]}...")

    except Exception as e:
        print(f"✗ Staging database failed: {e}")

    try:
        production = get_production_db()
        print("✓ Production database connected")

        result = production.execute("SELECT version();", fetch=True)
        print(f"  PostgreSQL version: {result[0]['version'][:50]}...")

    except Exception as e:
        print(f"✗ Production database failed: {e}")

    finally:
        close_all_connections()
