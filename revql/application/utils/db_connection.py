import sqlite3

class DatabaseConnection:
    _instance = None

    def __new__(cls, db_path):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance._db_path = db_path
            cls._instance._conn = sqlite3.connect(db_path)
            cls._instance._cursor = cls._instance._conn.cursor()
        return cls._instance

    @property
    def cursor(self):
        return self._instance._cursor

    def commit(self):
        self._instance._conn.commit()

    def close(self):
        self._instance._conn.close()
        DatabaseConnection._instance = None