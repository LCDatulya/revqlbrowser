from ..db_connection import DatabaseConnection
from ..db_utils import delete_empty_tables, delete_empty_columns
import sqlite3
import logging

class DatabaseCleaner:
    @staticmethod
    def cleanup_database(db_path: str) -> None:
        """Database cleanup logic"""
        db = None
        try:
            db = DatabaseConnection(db_path)
            
            # Delete empty tables
            deleted_tables = delete_empty_tables(db_path)
            if deleted_tables:
                logging.info(f"Deleted empty tables: {', '.join(deleted_tables)}")
            
            # Clean columns in remaining tables
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            for table in db.cursor.fetchall():
                table_name = table[0]
                if table_name != 'sqlite_sequence':
                    delete_empty_columns(db_path, table_name)
                    
            db.commit()
            
        except Exception as e:
            if db:
                db.rollback()
            logging.error(f"Error during cleanup: {e}")
            raise
        finally:
            if db:
                try:
                    db.close()
                except:
                    pass

    @staticmethod
    def cleanup_temp_tables(db: DatabaseConnection) -> None:
        """Drop temporary tables that match a naming convention (e.g., ending with '_temp_...')."""
        try:
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = db.cursor.fetchall()
            temp_tables = [table[0] for table in tables if "_temp_" in table[0]]
            for table in temp_tables:
                db.cursor.execute(f'DROP TABLE IF EXISTS "{table}"')
            db.commit()
            logging.info(f"Cleaned up temporary tables: {temp_tables}")
        except Exception as e:
            db.rollback()
            logging.error(f"Error cleaning up temporary tables: {e}")
            raise
            
    