from ..db_connection import DatabaseConnection
import sqlite3
import logging

class TransactionManager:
    def __init__(self):
        self.transaction_stack = {}  # db -> count

    def begin(self, db: DatabaseConnection) -> None:
        """Begin transaction with nesting support"""
        if db not in self.transaction_stack:
            self.transaction_stack[db] = 0
            db.cursor.execute("BEGIN TRANSACTION")
        self.transaction_stack[db] += 1

    def commit(self, db: DatabaseConnection) -> None:
        """Commit only if outermost transaction"""
        if db in self.transaction_stack:
            self.transaction_stack[db] -= 1
            if self.transaction_stack[db] == 0:
                db.commit()
                del self.transaction_stack[db]

    def rollback(self, db: DatabaseConnection) -> None:
        """Rollback and clear transaction stack"""
        if db in self.transaction_stack:
            db.rollback()
            del self.transaction_stack[db]

    def in_transaction(self, db: DatabaseConnection) -> bool:
        """Check if database is in transaction"""
        return db in self.transaction_stack