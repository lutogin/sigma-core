"""
TimescaleDB connection and management.

Provides connection pooling and basic database operations.
"""

import psycopg2
from psycopg2 import pool, extras
from contextlib import contextmanager
from typing import Optional


class TimescaleDB:
    """
    TimescaleDB connection manager with connection pooling.
    
    Usage:
        db = TimescaleDB(db_url, logger)
        db.connect()
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM ohlcv LIMIT 10")
                rows = cur.fetchall()
        
        db.close()
    """
    
    def __init__(self, db_url: str, logger):
        """
        Initialize TimescaleDB manager.
        
        :param db_url: PostgreSQL/TimescaleDB connection URL
        :param logger: Logger instance (DI)
        """
        self._db_url = db_url
        self.logger = logger
        self._pool: Optional[pool.ThreadedConnectionPool] = None
    
    @property
    def db_url(self) -> str:
        """Database URL."""
        return self._db_url
    
    def connect(self, min_connections: int = 1, max_connections: int = 10) -> None:
        """
        Initialize connection pool.
        
        :param min_connections: Minimum pool connections
        :param max_connections: Maximum pool connections
        """
        if self._pool is not None:
            return
        
        try:
            self._pool = pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                self.db_url
            )
            self.logger.info(f"✅ TimescaleDB connected (pool: {min_connections}-{max_connections})")
        except Exception as e:
            self.logger.error(f"❌ TimescaleDB connection failed: {e}")
            raise
    
    def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            self.logger.info("TimescaleDB connections closed")
    
    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Usage:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(...)
        """
        if self._pool is None:
            self.connect()
        
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)
    
    def execute(self, query: str, params: tuple = None) -> None:
        """
        Execute a single query (INSERT, UPDATE, DELETE, DDL).
        
        :param query: SQL query
        :param params: Query parameters
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
    
    def execute_many(self, query: str, params_list: list) -> int:
        """
        Execute query with multiple parameter sets (batch insert).
        
        :param query: SQL query with placeholders
        :param params_list: List of parameter tuples
        :return: Number of rows affected
        """
        if not params_list:
            return 0
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                extras.execute_batch(cur, query, params_list, page_size=1000)
                return cur.rowcount
    
    def fetch_one(self, query: str, params: tuple = None) -> Optional[tuple]:
        """
        Fetch a single row.
        
        :param query: SQL query
        :param params: Query parameters
        :return: Row tuple or None
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()
    
    def fetch_all(self, query: str, params: tuple = None) -> list:
        """
        Fetch all rows.
        
        :param query: SQL query
        :param params: Query parameters
        :return: List of row tuples
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
    
    def run_migration(self, migration_sql: str) -> None:
        """
        Run a migration script.
        
        :param migration_sql: SQL migration script
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(migration_sql)
        self.logger.info("Migration executed successfully")
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.
        
        :param table_name: Table name
        :return: True if exists
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """
        result = self.fetch_one(query, (table_name,))
        return result[0] if result else False
    
    def is_hypertable(self, table_name: str) -> bool:
        """
        Check if a table is a TimescaleDB hypertable.
        
        :param table_name: Table name
        :return: True if hypertable
        """
        query = """
            SELECT EXISTS (
                SELECT FROM timescaledb_information.hypertables
                WHERE hypertable_name = %s
            );
        """
        try:
            result = self.fetch_one(query, (table_name,))
            return result[0] if result else False
        except Exception:
            # TimescaleDB extension might not be installed
            return False

