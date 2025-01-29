import sqlite3

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