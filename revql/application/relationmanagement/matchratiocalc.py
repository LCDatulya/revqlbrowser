from ..utils.db_connection import DatabaseConnection
import sqlite3

def prefix_similarity(str1, str2):
    """Calculate similarity ratio between two strings."""
    str1 = str1.lower()
    str2 = str2.lower()
    
    # Handle special cases
    if str1 == str2:
        return 1.0
    
    # Find the longest common substring
    len1 = len(str1)
    len2 = len(str2)
    matrix = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    longest = 0
    
    for i in range(len1):
        for j in range(len2):
            if str1[i] == str2[j]:
                matrix[i + 1][j + 1] = matrix[i][j] + 1
                longest = max(longest, matrix[i + 1][j + 1])
    
    # Calculate similarity ratio
    similarity = (2.0 * longest) / (len1 + len2)
    
    # Boost score if one string starts with the other
    if str1.startswith(str2) or str2.startswith(str1):
        similarity = (similarity + 1.0) / 2.0
        
    return similarity

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
            
            # Skip system tables
            if table_name in ('sqlite_sequence', 'sqlite_master'):
                continue

            for t_name in table_names:
                # Skip comparing table to itself
                if table_name == t_name:
                    continue

                # Skip if column name is just the suffix _id of any table
                if any(column_name == f"{other_table}_id" for other_table in table_names):
                    continue

                # Special case for Phase relationships
                if (column_name.lower().startswith('phase') and t_name == 'Phases') or \
                   (column_name == "PhaseCreated" and t_name == "Phases"):
                    match_ratio = 1.0
                else:
                    # Calculate similarity
                    match_ratio = prefix_similarity(column_name.lower(), t_name.lower())

                print(f"Checking {table_name}.{column_name} against {t_name}: ratio = {match_ratio}")

                if match_ratio > 0.65:
                    try:
                        # Check if data in the matched column exists in the matching table
                        cursor.execute(f"SELECT DISTINCT \"{column_name}\" FROM \"{table_name}\" WHERE \"{column_name}\" IS NOT NULL")
                        column_data = set(str(item[0]) for item in cursor.fetchall() if item[0] is not None)

                        if not column_data:  # Skip if no data
                            continue

                        # Get ID columns from matching table
                        cursor.execute(f"PRAGMA table_info(\"{t_name}\");")
                        columns_info = cursor.fetchall()
                        id_columns = [col[1] for col in columns_info 
                                    if col[1].lower() == 'id' 
                                    or col[1].lower().endswith('id')]

                        if id_columns:
                            # Check for data overlap
                            for id_column in id_columns:
                                cursor.execute(f"SELECT DISTINCT \"{id_column}\" FROM \"{t_name}\" WHERE \"{id_column}\" IS NOT NULL")
                                id_data = set(str(item[0]) for item in cursor.fetchall() if item[0] is not None)

                                if column_data & id_data:  # If there's any overlap
                                    matching_info.append((table_name, column_name, t_name, match_ratio))
                                    print(f"Match found: {table_name}.{column_name} -> {t_name}.{id_column}")
                                    break

                    except sqlite3.Error as e:
                        print(f"Error checking {table_name}.{column_name}: {e}")
                        continue

    db.commit()
    return matching_info