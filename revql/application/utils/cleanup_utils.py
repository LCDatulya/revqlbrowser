import sqlite3
import logging
from revql.application.utils.db_connection import DatabaseConnection

def delete_empty_tables(db_path):
    """
    Delete empty tables from the database and return a list of deleted table names.
    """
    deleted_tables = []
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            
            # Skip sqlite_sequence table
            if table_name == 'sqlite_sequence':
                continue
            
            # Check if table is empty
            cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
            count = cursor.fetchone()[0]
            
            if count == 0:
                try:
                    cursor.execute(f"DROP TABLE \"{table_name}\"")
                    deleted_tables.append(table_name)
                except sqlite3.Error as e:
                    logging.warning(f"Error dropping table {table_name}: {e}")
                    continue
        
        conn.commit()
        return deleted_tables
        
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        return []
        
    finally:
        conn.close()

def delete_empty_columns(db_path: str, table_name: str) -> None:
    """Delete empty columns from a table, skipping primary keys and handling locks."""
    db = None
    try:
        db = DatabaseConnection(db_path)
        cursor = db.cursor
        
        # Get table info including primary key information
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = cursor.fetchall()
        
        # Create new table without empty columns
        temp_table = f"{table_name}_temp"
        keep_columns = []
        
        for col in columns:
            column_name = col[1]
            column_type = col[2]
            is_pk = col[5]  # Check if column is primary key
            
            # Skip primary keys and ProjectInformation_id
            if is_pk or column_name == "ProjectInformation_id":
                keep_columns.append((column_name, column_type))
                continue
            
            # Check if column is empty
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}" WHERE "{column_name}" IS NOT NULL')
            non_null_count = cursor.fetchone()[0]
            
            if non_null_count > 0:
                keep_columns.append((column_name, column_type))
        
        # Create new table with remaining columns
        create_columns = []
        for col_name, col_type in keep_columns:
            if col_name == f"{table_name}_id":
                create_columns.append(f'"{col_name}" {col_type} PRIMARY KEY AUTOINCREMENT')
            else:
                create_columns.append(f'"{col_name}" {col_type}')
        
        cursor.execute(f'''
            CREATE TABLE "{temp_table}" (
                {", ".join(create_columns)}
            )
        ''')
        
        # Copy data from old table to new table
        source_columns = [f'"{col[0]}"' for col in keep_columns]
        cursor.execute(f'''
            INSERT INTO "{temp_table}" ({", ".join(source_columns)})
            SELECT {", ".join(source_columns)}
            FROM "{table_name}"
        ''')
        
        # Drop old table and rename new table
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cursor.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')
        
        db.commit()
        
    except sqlite3.Error as e:
        if db:
            db.rollback()
        logging.warning(f"Error processing table {table_name}: {e}")
    finally:
        if db:
            db.close()