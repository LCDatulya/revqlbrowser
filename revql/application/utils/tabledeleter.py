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

def delete_single_column_or_row_tables(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    single_column_or_row_tables = []

    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\";")
        row_count = cursor.fetchone()[0]
        cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
        col_count = len(cursor.fetchall())
        if row_count == 1 or col_count == 1:
            single_column_or_row_tables.append(table_name)
            cursor.execute(f"DROP TABLE \"{table_name}\";")

    conn.commit()
    conn.close()
    return single_column_or_row_tables