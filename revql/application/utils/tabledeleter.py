import sqlite3

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