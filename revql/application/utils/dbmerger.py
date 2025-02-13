import sqlite3
import logging
import time
from typing import List, Set, Dict, Tuple
from ..utils.db_connection import DatabaseConnection
from ..utils.db_utils import delete_empty_tables, delete_empty_columns
from ..relationmanagement.projectmanagement import ensure_project_information_id

class DatabaseMerger:
    MAX_RETRIES = 3
    RETRY_DELAY = 0.5

    def __init__(self, source_db_path: str, target_db_path: str):
        self.source_db_path = source_db_path
        self.target_db_path = target_db_path
        self._temp_tables: Set[str] = set()

    def _execute_with_retry(self, db: DatabaseConnection, sql: str, params: tuple = None) -> None:
        """Execute SQL with retry logic for locked database"""
        for attempt in range(self.MAX_RETRIES):
            try:
                if params:
                    db.cursor.execute(sql, params)
                else:
                    db.cursor.execute(sql)
                return
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                    continue
                logging.error(f"OperationalError on attempt {attempt + 1} for SQL: {sql} with params: {params} - {e}")
                raise
            except sqlite3.Error as e:
                logging.error(f"SQLite error on attempt {attempt + 1} for SQL: {sql} with params: {params} - {e}")
                raise

    def _get_table_schema(self, db: DatabaseConnection, table_name: str) -> List[tuple]:
        """Get table schema with retry logic"""
        for attempt in range(self.MAX_RETRIES):
            try:
                db.cursor.execute(f"PRAGMA table_info('{table_name}')")
                return db.cursor.fetchall()
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                    continue
                logging.error(f"OperationalError on attempt {attempt + 1} for PRAGMA table_info('{table_name}') - {e}")
                raise
            except sqlite3.Error as e:
                logging.error(f"SQLite error on attempt {attempt + 1} for PRAGMA table_info('{table_name}') - {e}")
                raise

    def _setup_database(self, db: DatabaseConnection) -> None:
        """Configure database for better concurrency"""
        self._execute_with_retry(db, "PRAGMA journal_mode=WAL")
        self._execute_with_retry(db, "PRAGMA synchronous=NORMAL")
        self._execute_with_retry(db, "PRAGMA foreign_keys=ON")

    def cleanup_database(self, db_path: str) -> None:
        """Clean up empty tables and columns with improved error handling"""
        db = None
        try:
            db = DatabaseConnection(db_path)
            self._setup_database(db)
            
            # Delete empty tables
            deleted_tables = delete_empty_tables(db_path)
            if deleted_tables:
                logging.info(f"Deleted empty tables: {', '.join(deleted_tables)}")
            
            # Get remaining tables
            self._execute_with_retry(db, "SELECT name FROM sqlite_master WHERE type='table'")
            tables = db.cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                if table_name != 'sqlite_sequence':
                    self._cleanup_table_columns(db, table_name)
            
            db.commit()
            
        except Exception as e:
            if db:
                db.rollback()
            logging.error(f"Error during database cleanup: {e}")
            raise
        finally:
            if db:
                db.close()

    def _cleanup_table_columns(self, db: DatabaseConnection, table_name: str) -> None:
        """Clean up empty columns in a table"""
        try:
            columns = self._get_table_schema(db, table_name)
            if not columns:
                return

            # Identify columns to keep
            keep_columns = []
            for col in columns:
                col_name = col[1]
                if col[5] or col_name == "ProjectInformation_id":  # Primary key or special column
                    keep_columns.append(col)
                    continue

                # Check if column has data
                self._execute_with_retry(
                    db,
                    f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col_name}" IS NOT NULL',
                )
                if db.cursor.fetchone()[0] > 0:
                    keep_columns.append(col)

            # Recreate table if columns changed
            if len(keep_columns) < len(columns):
                self._recreate_table_with_columns(db, table_name, keep_columns)

        except sqlite3.Error as e:
            logging.warning(f"Error cleaning up columns in {table_name}: {e}")

    def _recreate_table_with_columns(self, db: DatabaseConnection, table_name: str, columns: List[tuple]) -> None:
        """Recreate table with specified columns"""
        temp_name = f"{table_name}_temp_{int(time.time())}"
        self._temp_tables.add(temp_name)

        try:
            # Create new table
            col_defs = []
            col_names = []
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                col_names.append(f'"{col_name}"')
                if col[5]:  # Is primary key
                    col_defs.append(f'"{col_name}" {col_type} PRIMARY KEY AUTOINCREMENT')
                else:
                    col_defs.append(f'"{col_name}" {col_type}')

            create_sql = f'CREATE TABLE "{temp_name}" ({", ".join(col_defs)})'
            self._execute_with_retry(db, create_sql)

            # Copy data
            copy_sql = f'''
                INSERT INTO "{temp_name}" ({", ".join(col_names)})
                SELECT {", ".join(col_names)}
                FROM "{table_name}"
            '''
            self._execute_with_retry(db, copy_sql)

            # Replace old table
            self._execute_with_retry(db, f'DROP TABLE "{table_name}"')
            self._execute_with_retry(db, f'ALTER TABLE "{temp_name}" RENAME TO "{table_name}"')
            self._temp_tables.remove(temp_name)
            db.commit()

        except Exception as e:
            db.rollback()
            if temp_name in self._temp_tables:
                self._execute_with_retry(db, f'DROP TABLE IF EXISTS "{temp_name}"')
                self._temp_tables.remove(temp_name)
            logging.error(f"Error recreating table {table_name} with columns {columns}: {e}")
            raise

    def merge_databases(self) -> bool:
        """Merge source database into target database with improved handling"""
        source_db = None
        target_db = None
        try:
            # Clean up databases
            self.cleanup_database(self.source_db_path)
            self.cleanup_database(self.target_db_path)

            # Setup connections
            source_db = DatabaseConnection(self.source_db_path)
            target_db = DatabaseConnection(self.target_db_path)
            self._setup_database(source_db)
            self._setup_database(target_db)

            # Process tables
            tables = self._get_source_tables(source_db)
            for table_name, columns in tables.items():
                try:
                    self._process_table(source_db, target_db, table_name, columns)
                except sqlite3.Error as e:
                    logging.error(f"Error processing table {table_name}: {e}")
                    continue

            # Ensure relationships
            ensure_project_information_id(self.target_db_path)
            target_db.commit()
            return True

        except Exception as e:
            if target_db:
                target_db.rollback()
            logging.error(f"Database merge error: {e}")
            return False

        finally:
            if source_db:
                source_db.close()
            if target_db:
                target_db.close()

    def _get_source_tables(self, db: DatabaseConnection) -> Dict[str, List[tuple]]:
        """Get all tables and their schemas from source database"""
        tables = {}
        self._execute_with_retry(
            db,
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('sqlite_sequence')"
        )
        for table in db.cursor.fetchall():
            table_name = table[0]
            columns = self._get_table_schema(db, table_name)
            if columns:
                tables[table_name] = columns
        return tables

    def _process_table(self, source_db: DatabaseConnection, target_db: DatabaseConnection, 
                      table_name: str, columns: List[tuple]) -> None:
        """Process a single table during merge"""
        target_exists = self._check_table_exists(target_db, table_name)
        
        if target_exists:
            self._merge_existing_table(source_db, target_db, table_name, columns)
        else:
            self._copy_table(source_db, target_db, table_name, columns)

    def _check_table_exists(self, db: DatabaseConnection, table_name: str) -> bool:
        """Check if table exists in database"""
        self._execute_with_retry(
            db,
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return db.cursor.fetchone() is not None
    
    def _copy_table(self, source_db: DatabaseConnection, target_db: DatabaseConnection, 
                    table_name: str, columns: List[tuple]) -> None:
        """Create new table and copy all data"""
        temp_name = f"{table_name}_temp_{int(time.time())}"
        self._temp_tables.add(temp_name)
        
        try:
            # Generate column definitions
            col_defs = []
            col_names = []
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                col_names.append(f'"{col_name}"')
                if col[5]:  # Is primary key
                    col_defs.append(f'"{col_name}" {col_type} PRIMARY KEY AUTOINCREMENT')
                else:
                    col_defs.append(f'"{col_name}" {col_type}')
            
            # Create table
            create_sql = f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})'
            self._execute_with_retry(target_db, create_sql)
            
            # Copy data
            self._execute_with_retry(
                source_db,
                f'SELECT * FROM "{table_name}"'
            )
            rows = source_db.cursor.fetchall()
            
            if rows:
                columns_str = ", ".join(col_names)
                placeholders = ",".join(["?" for _ in range(len(col_names))])
                insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
                
                for row in rows:
                    try:
                        self._execute_with_retry(target_db, insert_sql, row)
                    except sqlite3.IntegrityError as e:
                        logging.warning(f"Skipping duplicate record in {table_name}: {e}")
            
            target_db.commit()
            
        except Exception as e:
            target_db.rollback()
            logging.error(f"Error copying table {table_name} with columns {columns}: {e}")
            raise
        finally:
            if temp_name in self._temp_tables:
                self._temp_tables.remove(temp_name)

    def _merge_existing_table(self, source_db: DatabaseConnection, target_db: DatabaseConnection, 
                           table_name: str, source_columns: List[tuple]) -> None:
        """Merge data into existing table with duplicate column handling"""
        try:
            # Get target table schema and map column names to their definitions
            target_columns = self._get_table_schema(target_db, table_name)
            target_col_names = {col[1].lower() for col in target_columns}
        
            # For each source column, add it to target if missing (skip if exists)
            for col in source_columns:
                col_name = col[1]
                col_lower = col_name.lower()
                if col_lower in target_col_names:
                    continue  # Column already exists, skip
                try:
                    alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col[2]}'
                    self._execute_with_retry(target_db, alter_sql)
                except sqlite3.OperationalError as e:
                    if "duplicate column name" not in str(e):
                        logging.warning(f"Error adding column {col_name} to {table_name}: {e}")
        
            # Ensure ProjectInformation_id exists; add it only if missing
            if 'projectinformation_id' not in target_col_names:
                try:
                    alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "ProjectInformation_id" INTEGER'
                    self._execute_with_retry(target_db, alter_sql)
                except sqlite3.OperationalError as e:
                    if "duplicate column name" not in str(e):
                        logging.warning(f"Error adding ProjectInformation_id to {table_name}: {e}")
        
            # Retrieve existing rows from target to avoid duplicates
            self._execute_with_retry(
                target_db,
                f'SELECT "{table_name}_id" FROM "{table_name}"'
            )
            existing_ids = {row[0] for row in target_db.cursor.fetchall()}
        
            # Fetch all rows from source table
            self._execute_with_retry(
                source_db,
                f'SELECT * FROM "{table_name}"'
            )
            rows = source_db.cursor.fetchall()
            if not rows:
                return
        
            # Build column list for insertion from target schema
            # This list includes columns that are common to source and target.
            new_target_schema = self._get_table_schema(target_db, table_name)
            common_columns = [col[1] for col in new_target_schema if col[1].lower() in {sc[1].lower() for sc in source_columns}]
            columns_str = ", ".join(f'"{col}"' for col in common_columns)
            placeholders = ",".join("?" for _ in common_columns)
            insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
        
            # Insert rows that don't already exist in target
            for row in rows:
                # Assuming the first column is the primary key; adjust index as needed
                if row[0] in existing_ids:
                    continue
                try:
                    self._execute_with_retry(target_db, insert_sql, row)
                except sqlite3.IntegrityError as e:
                    logging.warning(f"Skipping duplicate record in {table_name}: {e}")
        
            target_db.commit()
        
        except Exception as e:
            target_db.rollback()
            logging.error(f"Error merging table {table_name} with columns {source_columns}: {e}")
            raise