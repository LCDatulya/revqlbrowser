import sqlite3

class DatabaseConnection:
    def __init__(self, db_path):
        self._conn = None
        self._cursor = None
        self._db_path = db_path
        self.connect()

    def connect(self):
        try:
            self._conn = sqlite3.connect(self._db_path)
            self._cursor = self._conn.cursor()
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            self._conn = None
            self._cursor = None

    def commit(self):
        if self._conn:
            self._conn.commit()

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            self._cursor = None