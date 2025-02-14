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
    def cleanup_temp_tables(self, db: DatabaseConnection) -> None:
        """Temp table cleanup logic"""
        try:
            db.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_temp_%'"
            )
            temp_tables = [row[0] for row in db.cursor.fetchall()]
            
            for temp_table in temp_tables:
                try:
                    db.cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
                    logging.info(f"Dropped temp table: {temp_table}")
                except sqlite3.Error as e:
                    logging.warning(f"Failed to drop temp table {temp_table}: {e}")
                    
        except sqlite3.Error as e:
            logging.error(f"Error cleaning temp tables: {e}")