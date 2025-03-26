from typing import Dict, List, Tuple
from ..db_connection import DatabaseConnection
import sqlite3
import logging
import time

def build_insert_statement(table_name, source_db, target_db, available_columns, 
                        target_columns, source_columns, id_mapping, target_pi_name=None):
    """
    Build INSERT statement for merged table with proper handling of ProjectInformation_id.
    """
    # Get column positions from source table
    source_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
    source_column_positions = {}
    source_pi_name = None  # Source column name for ProjectInformation_id
    
    for i, col in enumerate(source_db.cursor.fetchall()):
        col_name = col[1]
        col_lower = col_name.lower()
        source_column_positions[col_lower] = i
        if col_lower == 'projectinformation_id':
            source_pi_name = col_name
    
    # Use provided target ProjectInformation_id name or default
    if not target_pi_name:
        target_pi_name = 'ProjectInformation_id'
    
    # Build INSERT statement with proper column names (using target column names)
    columns_list = []
    for col_lower in available_columns:
        if col_lower == 'projectinformation_id':
            columns_list.append(f'"{target_pi_name}"')
        else:
            columns_list.append(f'"{target_columns[col_lower]["name"]}"')
    
    columns_sql = ', '.join(columns_list)
    
    # Create placeholders for the values
    placeholders = ', '.join('?' for _ in range(len(available_columns)))
    
    # Create INSERT OR IGNORE statement to avoid duplicates
    insert_sql = f'INSERT OR IGNORE INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'
    
    # Get data from source table
    source_db.cursor.execute(f'SELECT * FROM "{table_name}"')
    rows = source_db.cursor.fetchall()
    
    # Process data for insertion
    batch_data = []
    for row in rows:
        values = []
        for col_lower in available_columns:
            pos = source_column_positions.get(col_lower)
            if pos is not None:
                val = row[pos]
                # CRITICAL FIX: Only map ProjectInformation_id if it's in the mapping
                # AND we're using a different ID (conflicts only)
                if col_lower == 'projectinformation_id' and val is not None:
                    mapped_val = id_mapping.get(val, val)  # Use original ID if not in mapping
                    values.append(mapped_val)
                else:
                    values.append(val)
            else:
                values.append(None)
        
        batch_data.append(tuple(values))
    
    return insert_sql, batch_data

class TableOperations:
    MAX_RETRIES = 3
    RETRY_DELAY = 0.5
    
    @staticmethod
    def copy_table(source_db: DatabaseConnection, target_db: DatabaseConnection, 
               table_name: str, columns: List[tuple], id_mapping: Dict[int, int]) -> None:
        """
        Copy a table from the source database to the target database.
        """
        temp_name = f"{table_name}_temp_{int(time.time())}"

        try:
            logging.info(f"Copying table {table_name}")

            # Generate column definitions
            col_defs = []
            col_names = []
            processed_cols = set()
            has_project_info_id = False
            pi_col_name = None

            for i, col in enumerate(columns):
                col_name = col[1]
                col_lower = col_name.lower()

                # Skip duplicate columns (case-insensitive)
                if col_lower in processed_cols:
                    continue
                
                processed_cols.add(col_lower)

                # Track if ProjectInformation_id exists
                if col_lower == 'projectinformation_id':
                    has_project_info_id = True
                    pi_col_name = col_name

                col_type = col[2]
                col_names.append(f'"{col_name}"')

                # Define column with appropriate constraints
                if col[5]:  # Is primary key
                    col_defs.append(f'"{col_name}" {col_type} PRIMARY KEY')
                else:
                    col_defs.append(f'"{col_name}" {col_type}')

            # Ensure ProjectInformation_id exists exactly once
            if not has_project_info_id:
                pi_col_name = "ProjectInformation_id"
                col_defs.append(f'"{pi_col_name}" INTEGER')
                col_names.append(f'"{pi_col_name}"')

            # Create temporary table in the target database
            try:
                create_sql = f'CREATE TABLE "{temp_name}" ({", ".join(col_defs)})'
                target_db.cursor.execute(create_sql)
            except sqlite3.OperationalError as e:
                logging.warning(f"Could not create table: {e}")
                raise
            
            # Get data from source table with column positions
            source_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
            source_columns = {col[1].lower(): (i, col[1]) for i, col in enumerate(source_db.cursor.fetchall())}

            source_db.cursor.execute(f'SELECT * FROM "{table_name}"')
            rows = source_db.cursor.fetchall()

            # Prepare data with ProjectInformation_id mapping
            prepared_data = []

            for row in rows:
                # Convert row to list for modification
                row_list = list(row)
                values = []

                # Check for ProjectInformation_id in source
                pi_value = None
                pi_found = False

                if has_project_info_id and 'projectinformation_id' in source_columns:
                    pi_idx, _ = source_columns['projectinformation_id']
                    if pi_idx < len(row_list):
                        pi_found = True
                        old_id = row_list[pi_idx]
                        # CRITICAL FIX: Only map if old_id is in the mapping AND has a different mapping
                        if old_id in id_mapping and id_mapping[old_id] != old_id:
                            pi_value = id_mapping[old_id]
                        else:
                            pi_value = old_id  # Preserve original ID

                # Build new row values matching the target schema
                for i, col in enumerate(columns):
                    col_name = col[1]
                    col_lower = col_name.lower()

                    # Skip duplicate or processed columns
                    if col_lower not in processed_cols:
                        continue
                    
                    # Handle ProjectInformation_id specially
                    if col_lower == 'projectinformation_id':
                        if pi_found:
                            values.append(pi_value)
                        else:
                            # Get default ProjectInformation_id if we need one
                            if pi_value is None:
                                target_db.cursor.execute('''
                                    SELECT "ProjectInformation_id" 
                                    FROM "ProjectInformation" 
                                    ORDER BY "ProjectInformation_id" DESC 
                                    LIMIT 1
                                ''')
                                result = target_db.cursor.fetchone()
                                pi_value = result[0] if result else 1
                            values.append(pi_value)
                    else:
                        # Get value from source if column exists there
                        if col_lower in source_columns:
                            idx, _ = source_columns[col_lower]
                            if idx < len(row_list):
                                values.append(row_list[idx])
                            else:
                                values.append(None)
                        else:
                            values.append(None)

                # Ensure values list matches columns list in length
                if len(values) == len(col_names):
                    prepared_data.append(tuple(values))

            # Insert data into temporary table in the target database
            if prepared_data:
                placeholders = ', '.join('?' for _ in range(len(col_names)))
                insert_sql = f'INSERT INTO "{temp_name}" ({", ".join(col_names)}) VALUES ({placeholders})'

                # Break into smaller batches to avoid SQLite limits
                batch_size = 500
                for i in range(0, len(prepared_data), batch_size):
                    batch = prepared_data[i:i+batch_size]
                    target_db.cursor.executemany(insert_sql, batch)

                target_db.commit()

            # Replace original table with the temporary one
            target_db.cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            target_db.cursor.execute(f'ALTER TABLE "{temp_name}" RENAME TO "{table_name}"')
            target_db.commit()

            logging.info(f"Successfully copied table {table_name}")

        except Exception as e:
            logging.error(f"Error copying table {table_name}: {e}", exc_info=True)
            # Try to clean up the temporary table if it exists
            try:
                target_db.cursor.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
                target_db.commit()
            except:
                pass
            raise

    # FIX: Corrected indentation - this method was incorrectly indented inside copy_table
    @staticmethod
    def merge_existing_table(source_db: DatabaseConnection, target_db: DatabaseConnection, 
                             table_name: str, columns: List[tuple], id_mapping: Dict[int, int]) -> None:
        """Merge table logic handling duplicate columns and adding missing columns"""
        try:
            logging.info(f"Merging existing table {table_name}")

            # Get source columns (using lower-case keys for consistency)
            source_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
            source_columns = {}
            for col in source_db.cursor.fetchall():
                col_name = col[1]
                col_lower = col_name.lower()
                if col_lower not in source_columns:
                    source_columns[col_lower] = {
                        'name': col_name,
                        'type': col[2],
                        'notnull': col[3],
                        'pk': col[5]
                    }

            # Get target columns (using lower-case keys)
            target_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
            target_columns = {}
            target_project_info_id_name = None  # Track actual column name for ProjectInformation_id
            for col in target_db.cursor.fetchall():
                col_name = col[1]
                col_lower = col_name.lower()
                if col_lower not in target_columns:
                    target_columns[col_lower] = {
                        'name': col_name,
                        'type': col[2],
                        'notnull': col[3],
                        'pk': col[5]
                    }
                    if col_lower == 'projectinformation_id':
                        target_project_info_id_name = col_name

            # For every column in the source that is missing in the target, add it
            # BUT avoid adding duplicate ProjectInformation_id
            for col_lower, info in source_columns.items():
                if col_lower != 'projectinformation_id' and col_lower not in target_columns:
                    try:
                        col_def = f'"{info["name"]}" {info["type"]}'
                        if info["notnull"]:
                            col_def += ' NOT NULL DEFAULT ""'
                        alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN {col_def}'
                        logging.info(f"Adding missing column to {table_name}: {alter_sql}")
                        target_db.cursor.execute(alter_sql)
                        target_db.commit()
                        target_columns[col_lower] = info
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not add column {info['name']}: {e}")

            # Ensure ProjectInformation_id exists (add only if missing)
            if 'projectinformation_id' not in target_columns:
                try:
                    logging.info(f"Adding missing ProjectInformation_id column to {table_name}")
                    target_db.cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "ProjectInformation_id" INTEGER')
                    target_db.commit()
                    target_columns['projectinformation_id'] = {
                        'name': 'ProjectInformation_id',
                        'type': 'INTEGER',
                        'notnull': 0,
                        'pk': 0
                    }
                    target_project_info_id_name = 'ProjectInformation_id'
                except sqlite3.OperationalError as e:
                    logging.warning(f"Could not add ProjectInformation_id column: {e}")

            # Build the INSERT statement using columns common to both tables
            # IMPORTANT: Handle ProjectInformation_id specially to avoid duplicates
            available_columns = []
            has_project_info_id = False

            for col_lower in source_columns.keys():
                if col_lower == 'projectinformation_id':
                    if not has_project_info_id:
                        has_project_info_id = True
                        available_columns.append(col_lower)
                elif col_lower in target_columns and col_lower != 'rowid':
                    available_columns.append(col_lower)

            if not available_columns:
                logging.error(f"No matching columns found for table {table_name}")
                return

            # Process data in batches
            insert_sql, batch_data = build_insert_statement(
                table_name, source_db, target_db, available_columns, 
                target_columns, source_columns, id_mapping,
                target_project_info_id_name
            )

            try:
                target_db.cursor.executemany(insert_sql, batch_data)
                target_db.commit()
                logging.info(f"Successfully merged table {table_name}")
            except sqlite3.Error as e:
                logging.error(f"Error inserting data into {table_name}: {e}")
                target_db.rollback()

        except Exception as e:
            target_db.rollback()
            logging.error(f"Error merging table {table_name}: {e}", exc_info=True)
            raise
        
    @staticmethod
    def table_exists(db: DatabaseConnection, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            result = db.cursor.fetchone()
            return result is not None
        except Exception as e:
            logging.error(f"Error checking if table exists: {e}")
            return False

    @staticmethod
    def ensure_column_exists(db: DatabaseConnection, table_name: str, column_name: str,
                            column_type: str = "INTEGER") -> bool:
        """
        Ensure a column exists in the specified table.
        Returns True if column was added or already exists, False otherwise.
        """
        try:
            # Check if column exists
            db.cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns = [col[1].lower() for col in db.cursor.fetchall()]
            
            if column_name.lower() not in columns:
                # Add column if it doesn't exist
                db.cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}')
                db.commit()
                logging.info(f"Added column '{column_name}' to table '{table_name}'")
            
            return True
        except sqlite3.Error as e:
            db.rollback()
            logging.error(f"Error ensuring column exists: {e}")
            return False