import sqlite3

def delete_empty_tables(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    empty_tables = []

    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\";")
        count = cursor.fetchone()[0]
        if count == 0:
            empty_tables.append(table_name)
            cursor.execute(f"DROP TABLE \"{table_name}\";")

    conn.commit()
    conn.close()
    return empty_tables