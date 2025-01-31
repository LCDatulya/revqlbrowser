import sqlite3
import time
import logging
from revql.application.utils.db_connection import DatabaseConnection
from typing import Dict, Set

class RenameTracker:
    def __init__(self):
        self.renamed_columns: Dict[str, Set[str]] = {}
    
    def was_renamed(self, table: str, column: str) -> bool:
        return table in self.renamed_columns and column in self.renamed_columns[table]
    
    def track_rename(self, table: str, column: str):
        if table not in self.renamed_columns:
            self.renamed_columns[table] = set()
        self.renamed_columns[table].add(column)

def rename_id_columns(db, tracker: RenameTracker):
    cursor = db.cursor
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
        columns = cursor.fetchall()
        existing_columns = {col[1].lower() for col in columns}

        for column in columns:
            column_name = column[1]
            if column_name.lower() == 'id':
                new_column_name = f"{table_name}_id"
                if (new_column_name.lower() not in existing_columns and 
                    not tracker.was_renamed(table_name, new_column_name)):
                    try:
                        cursor.execute(f"""
                            ALTER TABLE "{table_name}" 
                            RENAME COLUMN "{column_name}" TO "{new_column_name}";
                        """)
                        tracker.track_rename(table_name, new_column_name)
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not rename column in {table_name}: {e}")
    db.commit()

def table_has_constraints(db, table_name: str, new_column_name: str, 
                         match_table: str, match_table_id_column: str) -> bool:
    cursor = db.cursor
    cursor.execute(f'PRAGMA table_info("{table_name}");')
    columns = cursor.fetchall()
    if new_column_name not in {col[1] for col in columns}:
        return False

    cursor.execute(f'PRAGMA foreign_key_list("{table_name}");')
    foreign_keys = cursor.fetchall()
    return any(
        fk[3] == new_column_name and 
        fk[2] == match_table and 
        fk[4] == match_table_id_column 
        for fk in foreign_keys
    )

def execute_with_retry(db, query: str, params=(), max_retries=5, delay=1):
    cursor = db.cursor
    for attempt in range(max_retries):
        try:
            cursor.execute(query, params)
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                time.sleep(delay)
                continue
            raise

def rename_id_columns_and_create_relations(db_path: str, matching_info):
    db = DatabaseConnection(db_path)
    tracker = RenameTracker()
    
    try:
        # First rename all id columns
        rename_id_columns(db, tracker)
        
        # Then create relations
        for table_name, column_name, match_table, _ in matching_info:
            cursor = db.cursor
            
            # Skip if table is trying to reference itself
            if table_name == match_table:
                continue
            
            # Verify match table exists and has id or primary key column
            cursor.execute(f'PRAGMA table_info("{match_table}");')
            match_columns = cursor.fetchall()
            match_table_id = next((col[1] for col in match_columns if col[1].lower() == 'id' or col[5] == 1), None)
            
            if not match_table_id:
                continue
                
            # Skip if constraints already exist
            if table_has_constraints(db, table_name, match_table_id, 
                                  match_table, match_table_id):
                continue
                
            # Create new table with foreign key and primary key
            cursor.execute(f'PRAGMA table_info("{table_name}");')
            current_columns = cursor.fetchall()
            
            # Find or create primary key column
            table_id_column = f"{table_name}_id"
            has_primary_key = any(col[5] == 1 for col in current_columns)  # Check if any column is primary key
            
            column_defs = []
            if not has_primary_key:
                # Add primary key as first column if none exists
                column_defs.append(f'"{table_id_column}" INTEGER PRIMARY KEY AUTOINCREMENT')
            
            # Add all existing columns except the one being replaced
            column_defs.extend([
                f'"{col[1]}" {col[2]} {"PRIMARY KEY" if col[5] == 1 else ""}' 
                for col in current_columns 
                if col[1] != column_name
            ])
            
            # Add the foreign key column
            column_defs.append(f'"{match_table_id}" INTEGER REFERENCES "{match_table}"("{match_table_id}")')
            
            new_table_name = f"{table_name}_new"
            create_sql = f"""
                CREATE TABLE "{new_table_name}" (
                    {', '.join(column_defs)}
                );
            """
            
            try:
                # Create new table
                execute_with_retry(db, create_sql)
                
                # Copy data
                source_cols = [
                    f'"{col[1]}"' 
                    for col in current_columns 
                    if col[1] != column_name
                ]
                
                # Include new primary key column in INSERT if it was added
                if not has_primary_key:
                    source_cols.insert(0, '(SELECT COALESCE(MAX("{table_id_column}"), 0) + ROW_NUMBER() OVER () FROM "{table_name}")')
                
                insert_sql = f"""
                    INSERT INTO "{new_table_name}" ({', '.join(source_cols)}, "{match_table_id}")
                    SELECT {', '.join(source_cols)},
                        (SELECT "{match_table_id}" 
                         FROM "{match_table}" 
                         WHERE "{match_table}"."{match_table_id}" = "{table_name}"."{column_name}")
                    FROM "{table_name}";
                """
                execute_with_retry(db, insert_sql)
                
                # Replace old table
                execute_with_retry(db, f'DROP TABLE "{table_name}";')
                execute_with_retry(db, f'ALTER TABLE "{new_table_name}" RENAME TO "{table_name}";')
                
            except sqlite3.Error as e:
                logging.error(f"Error creating relation for {table_name}: {e}")
                # Clean up on failure
                execute_with_retry(db, f'DROP TABLE IF EXISTS "{new_table_name}";')
                continue
                
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise