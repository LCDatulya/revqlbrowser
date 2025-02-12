import sqlite3
from typing import Optional

class DatabaseConnection:
    def __init__(self, db_path: str):
        """Initialize database connection"""
        self._connection: Optional[sqlite3.Connection] = None
        self._cursor: Optional[sqlite3.Cursor] = None
        self._db_path = db_path
        self._connect()

    def _connect(self):
        """Establish database connection"""
        try:
            self._connection = sqlite3.connect(self._db_path)
            self._cursor = self._connection.cursor()
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to connect to database: {e}")

    @property
    def cursor(self) -> sqlite3.Cursor:
        """Get the database cursor"""
        if not self._cursor:
            self._connect()
        return self._cursor

    def commit(self):
        """Commit the current transaction"""
        if self._connection:
            self._connection.commit()

    def rollback(self):
        """Rollback the current transaction"""
        if self._connection:
            self._connection.rollback()

    def close(self):
        """Close the database connection"""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        self._cursor = None
        self._connection = None

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def execute(self, query: str, parameters: tuple = ()) -> sqlite3.Cursor:
        """Execute a query with parameters"""
        return self.cursor.execute(query, parameters)

    def executemany(self, query: str, parameters: list) -> sqlite3.Cursor:
        """Execute many queries with parameters"""
        return self.cursor.executemany(query, parameters)

    def fetchone(self):
        """Fetch one row from the cursor"""
        return self.cursor.fetchone()

    def fetchall(self):
        """Fetch all rows from the cursor"""
        return self.cursor.fetchall()

    @property
    def connection(self) -> sqlite3.Connection:
        """Get the database connection"""
        if not self._connection:
            self._connect()
        return self._connection