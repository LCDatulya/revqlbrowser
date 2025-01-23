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
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        table_data.append((table_name, count))

    conn.close()
    return table_data