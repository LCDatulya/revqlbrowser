from .transactionmanager import TransactionManager
from .tableoperations import TableOperations
from .projectinformationhandler import ProjectInformationHandler
from .mergeddatabasecleaner import DatabaseCleaner
from ..db_connection import DatabaseConnection
import logging
import sqlite3

class DatabaseMerger:
    def __init__(self, source_db_path: str, target_db_path: str):
        self.source_db_path = source_db_path
        self.target_db_path = target_db_path
        self.transaction_manager = TransactionManager()
        self.table_ops = TableOperations()
        self.project_info = ProjectInformationHandler()
        self.cleaner = DatabaseCleaner()

    def merge_databases(self) -> bool:
        source_db = None
        target_db = None
        
        try:
            # Initial cleanup
            self.cleaner.cleanup_database(self.source_db_path)
            self.cleaner.cleanup_database(self.target_db_path)
            
            # Setup connections
            source_db = DatabaseConnection(self.source_db_path)
            target_db = DatabaseConnection(self.target_db_path)
            
            # Begin transaction
            self.transaction_manager.begin(target_db)
            
            # Setup and merge ProjectInformation
            self.project_info.ensure_project_information_table(target_db)
            id_mapping = self.project_info.merge_project_information(source_db, target_db)
            
            # Process remaining tables
            source_db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            for table in source_db.cursor.fetchall():
                table_name = table[0]
                if table_name not in ('ProjectInformation', 'sqlite_sequence'):
                    try:
                        source_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
                        columns = source_db.cursor.fetchall()
                        
                        if self.table_ops.table_exists(target_db, table_name):
                            self.table_ops.merge_existing_table(
                                source_db, target_db, table_name, columns, id_mapping
                            )
                        else:
                            self.table_ops.copy_table(
                                source_db, target_db, table_name, columns, id_mapping
                            )
                    except Exception as e:
                        logging.error(f"Error processing table {table_name}: {e}")
                        continue
                        
            # Update sequences and cleanup
            self.project_info.update_sequences(target_db)
            self.cleaner.cleanup_temp_tables(target_db)
            self.transaction_manager.commit(target_db)
            return True
            
        except Exception as e:
            if target_db:
                self.transaction_manager.rollback(target_db)
            logging.error(f"Database merge error: {e}")
            return False
            
        finally:
            if source_db:
                source_db.close()
            if target_db:
                if self.transaction_manager.in_transaction(target_db):
                    self.transaction_manager.rollback(target_db)
                target_db.close()