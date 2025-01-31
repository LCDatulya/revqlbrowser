from ..utils.db_connection import DatabaseConnection

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
    db = DatabaseConnection(db_path)
    cursor = db.cursor

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
                if match_ratio > 0.35:
                    # Check if data in the matched column exists in the id or primary key column of the matching table
                    cursor.execute(f"SELECT {column_name} FROM {table_name}")
                    column_data = cursor.fetchall()
                    column_data = [item[0] for item in column_data]

                    # Check if the id or primary key column exists in the matching table
                    cursor.execute(f"PRAGMA table_info(\"{t_name}\");")
                    columns_info = cursor.fetchall()
                    id_columns = [col[1] for col in columns_info if col[1].lower() == 'id' or col[5] == 1]

                    if id_columns:
                        id_data = []
                        for id_column in id_columns:
                            cursor.execute(f"SELECT {id_column} FROM {t_name}")
                            id_data.extend([item[0] for item in cursor.fetchall()])

                        # Check if there is an actual relation by verifying data alignment
                        matching_rows = sum(1 for data in column_data if data in id_data)
                        if matching_rows / len(column_data) > 0.35:
                            key = (table_name, column_name, t_name, match_ratio)
                            if key not in matching_info or match_ratio > matching_info[key]:
                                matching_info[key] = match_ratio

    return matching_info