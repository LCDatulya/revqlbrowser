import sqlite3
import logging
from revql.application.utils.db_connection import DatabaseConnection
from revql.application.utils.cleanup_utils import delete_empty_tables, delete_empty_columns
from typing import List, Tuple, Dict
from ..relationmanagement.matchratiocalc import get_overlap_percentage
from ..relationmanagement.matchratiocalc import prefix_similarity

def find_matching_table_column_names(db_path):
    db = DatabaseConnection(db_path)
    cursor = db._cursor

    # Step 1: Delete empty tables
    logging.info("Deleting empty tables...")
    delete_empty_tables(db_path)

    # Step 2: Delete empty columns from all tables
    logging.info("Deleting empty columns...")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        if table_name != 'sqlite_sequence':  # Skip system tables
            delete_empty_columns(db_path, table_name)

    # Step 3: Get the list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_names = [table[0] for table in tables]
    matching_info = []
    data_matching_info = []

    # Step 4: Find matching table-column names
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

                # Use prefix similarity to match column_name with table_name
                match_ratio = prefix_similarity(column_name.lower(), t_name.lower())
                if match_ratio < 0.8:  # Skip if similarity is below 80%
                    continue

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

                            overlap_percentage = get_overlap_percentage(column_data, id_data)
                            
                            # Match if overlap is 95% or higher
                            if overlap_percentage >= 95.0:
                                data_matching_info.append((table_name, column_name, t_name, match_ratio, overlap_percentage))
                                logging.info(f"Match found: {table_name}.{column_name} -> {t_name}.{id_column} (Overlap: {overlap_percentage:.2f}%)")
                            else:
                                logging.info(f"No match: {table_name}.{column_name} -> {t_name}.{id_column} (Overlap: {overlap_percentage:.2f}%)")
                            break

                except sqlite3.Error as e:
                    logging.error(f"Error checking {table_name}.{column_name}: {e}")
                    continue

    db.commit()
    return matching_info, data_matching_info

def get_table_data(db_path):
    """
    Retrieve table data including table name, row count, and column count.
    """
    db = DatabaseConnection(db_path)
    cursor = db.cursor

    try:
        # Get the list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        table_data = []
        for table in tables:
            table_name = table[0]
            if table_name in ['sqlite_sequence', 'sqlite_master']:
                continue

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
            row_count = cursor.fetchone()[0]

            # Get column count
            cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
            column_count = len(cursor.fetchall())

            table_data.append((table_name, row_count, column_count))

        return table_data

    except sqlite3.Error as e:
        logging.error(f"Error retrieving table data: {e}")
        return []

    finally:
        db.close()
        
def count_tables(db_path):
    """
    Count the number of tables in the database.
    """
    db = DatabaseConnection(db_path)
    cursor = db.cursor

    try:
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table';")
        return cursor.fetchone()[0]

    except sqlite3.Error as e:
        logging.error(f"Error counting tables: {e}")
        return 0

    finally:
        db.close()