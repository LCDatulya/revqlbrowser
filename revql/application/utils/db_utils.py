import sqlite3

def get_table_data(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_data = []

    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\";")
        row_count = cursor.fetchone()[0]
        cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
        col_count = len(cursor.fetchall())
        table_data.append((table_name, row_count, col_count))

    conn.close()
    return table_data

def count_tables(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    conn.close()
    return len(tables)

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
                    print(f"Error dropping table {table_name}: {e}")
                    continue
        
        conn.commit()
        return deleted_tables
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
        
    finally:
        conn.close()
        
def delete_empty_columns(db_path, table_name):
    # Skip sqlite_sequence table
    if table_name == 'sqlite_sequence':
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the column names
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]

    # Check each column for emptiness
    empty_columns = []
    for column in columns:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NOT NULL AND {column} != ''")
        count = cursor.fetchone()[0]
        if count == 0:
            empty_columns.append(column)

    # Delete empty columns
    for column in empty_columns:
        cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column}")

    conn.commit()
    conn.close()