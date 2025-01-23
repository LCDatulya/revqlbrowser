import sqlite3

def prefix_similarity(str1, str2):
    """Calculate the similarity ratio based on the common prefix length."""
    common_length = 0
    for c1, c2 in zip(str1, str2):
        if c1 == c2:
            common_length += 1
        else:
            break
    return common_length / max(len(str1), len(str2))

def find_matching_table_column_names(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_names = [table[0] for table in tables]
    matching_info = {}
    
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
        columns = cursor.fetchall()
        
        for column in columns:
            column_name = column[1]
            for t_name in table_names:
                match_ratio = prefix_similarity(column_name, t_name)
                if match_ratio > 0.65:
                    key = (table_name, column_name, t_name)
                    if key not in matching_info or match_ratio > matching_info[key]:
                        matching_info[key] = match_ratio

    conn.close()
    return [(table, column, match_table, ratio) for (table, column, match_table), ratio in matching_info.items()]