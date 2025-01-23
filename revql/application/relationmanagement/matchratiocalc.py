import sqlite3
from difflib import SequenceMatcher

def find_matching_table_column_names(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_names = [table[0] for table in tables]
    matching_info = []

    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
        columns = cursor.fetchall()
        
        for column in columns:
            column_name = column[1]
            for t_name in table_names:
                match_ratio = SequenceMatcher(None, column_name, t_name).ratio()
                if match_ratio > 0.55:
                    matching_info.append((table_name, column_name, t_name, match_ratio))

    conn.close()
    return matching_info