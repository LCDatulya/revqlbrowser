import sqlite3
import time

def rename_id_columns(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
        columns = cursor.fetchall()

        for column in columns:
            column_name = column[1]
            new_column_name = f"{table_name}_id"
            if column_name.lower() == 'id' and new_column_name not in [col[1] for col in columns]:
                cursor.execute(f"ALTER TABLE \"{table_name}\" RENAME COLUMN \"{column_name}\" TO \"{new_column_name}\";")

    conn.commit()
    conn.close()

def table_has_constraints(cursor, table_name, new_column_name, match_table, match_table_id_column):
    cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]

    if new_column_name not in column_names:
        return False

    cursor.execute(f"PRAGMA foreign_key_list(\"{table_name}\");")
    foreign_keys = cursor.fetchall()
    for fk in foreign_keys:
        if fk[3] == new_column_name and fk[2] == match_table and fk[4] == match_table_id_column:
            return True

    return False

def table_exists(cursor, table_name):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cursor.fetchone() is not None

def execute_with_retry(cursor, query, params=(), retries=5, delay=1):
    for attempt in range(retries):
        try:
            cursor.execute(query, params)
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                time.sleep(delay)
            else:
                raise
    raise sqlite3.OperationalError("Failed to execute query after multiple retries due to database lock.")

def rename_id_columns_and_create_relations(db_path, matching_info):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    rename_id_columns(db_path)

    for table_name, column_name, match_table, match_ratio in matching_info:
        new_column_name = f"{match_table}_id"
        cursor.execute(f"PRAGMA table_info(\"{match_table}\");")
        match_table_columns = cursor.fetchall()
        match_table_id_column = None

        for col in match_table_columns:
            if col[1].lower() == 'id' or col[1].lower().endswith('id'):
                match_table_id_column = col[1]
                break

        if match_table_id_column and not table_has_constraints(cursor, table_name, new_column_name, match_table, match_table_id_column):
            new_table_name = f"{table_name}_new"
            if not table_exists(cursor, new_table_name):
                # Create a new table with the foreign key constraint
                cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
                columns = cursor.fetchall()
                column_definitions = ", ".join([f"\"{col[1]}\" {col[2]}" for col in columns if col[1] != column_name])
                execute_with_retry(cursor, f"""
                    CREATE TABLE \"{new_table_name}\" (
                        {column_definitions},
                        \"{new_column_name}\" INTEGER,
                        FOREIGN KEY (\"{new_column_name}\") REFERENCES \"{match_table}\"(\"{match_table_id_column}\")
                    );
                """)
                execute_with_retry(cursor, f"""
                    INSERT INTO \"{new_table_name}\" ({', '.join([col[1] for col in columns if col[1] != column_name])}, \"{new_column_name}\")
                    SELECT {', '.join([col[1] for col in columns if col[1] != column_name])}, (
                        SELECT \"{match_table_id_column}\"
                        FROM \"{match_table}\"
                        WHERE \"{match_table}\".\"{match_table_id_column}\" = \"{table_name}\".\"{column_name}\"
                    )
                    FROM \"{table_name}\";
                """)
                execute_with_retry(cursor, f"DROP TABLE \"{table_name}\";")
                execute_with_retry(cursor, f"ALTER TABLE \"{new_table_name}\" RENAME TO \"{table_name}\";")

    conn.commit()
    conn.close()