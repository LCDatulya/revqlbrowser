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
    cursor = db._cursor

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
                # Ignore if column name is just the suffix _id of the table name
                if column_name == f"{t_name}_id":
                    continue

                # Ensure "PhaseCreated" is related to "Phases" table's id column
                if column_name == "PhaseCreated" and t_name == "Phases":
                    match_ratio = 1.0
                else:
                    match_ratio = prefix_similarity(column_name, t_name)

                if match_ratio > 0.65:
                    # Check if data in the matched column exists in the id or Id column of the matching table
                    cursor.execute(f"SELECT \"{column_name}\" FROM \"{table_name}\"")
                    column_data = cursor.fetchall()
                    column_data = [item[0] for item in column_data]

                    # Check if the id or Id column exists in the matching table
                    cursor.execute(f"PRAGMA table_info(\"{t_name}\");")
                    columns_info = cursor.fetchall()
                    id_columns = [col[1] for col in columns_info if col[1].lower() == 'id' or col[1].lower().endswith('id')]

                    if id_columns:
                        id_data = []
                        for id_column in id_columns:
                            cursor.execute(f"SELECT \"{id_column}\" FROM \"{t_name}\"")
                            id_data.extend([item[0] for item in cursor.fetchall()])

                        if any(data in id_data for data in column_data):
                            # Rename the column to match the table name
                            new_column_name = f"{t_name}_id"
                            cursor.execute(f"ALTER TABLE \"{table_name}\" RENAME COLUMN \"{column_name}\" TO \"{new_column_name}\"")
                            print(f"Match found and column renamed: {table_name}.{column_name} -> {t_name}.{new_column_name}")
                            matching_info.append((table_name, new_column_name, t_name, match_ratio))

    db.commit()
    return matching_info