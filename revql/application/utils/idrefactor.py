import sqlite3
import time
from .db_connection import DatabaseConnection

def rename_id_columns(db):
    cursor = db.cursor
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
        columns = cursor.fetchall()

        for column in columns:
            column_name = column[1]
            if column_name.lower() == 'id':
                new_column_name = f"{table_name}_id"
                if new_column_name not in [col[1] for col in columns]:
                    cursor.execute(f"ALTER TABLE \"{table_name}\" RENAME COLUMN \"{column_name}\" TO \"{new_column_name}\";")
    
    db.commit()

def table_has_constraints(db, table_name, new_column_name, match_table, match_table_id_column):
    cursor = db.cursor
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

def table_exists(db, table_name):
    cursor = db.cursor
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cursor.fetchone() is not None

def execute_with_retry(db, query, params=(), retries=5, delay=1):
    cursor = db.cursor
    for _ in range(retries):
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
    db = DatabaseConnection(db_path)
    cursor = db.cursor
    
    # First rename all id columns
    rename_id_columns(db)
    
    # Then create relations
    for table_name, column_name, match_table, match_ratio in matching_info:
        cursor.execute(f"PRAGMA table_info(\"{match_table}\");")
        match_table_columns = cursor.fetchall()
        match_table_id_column = f"{match_table}_id"

        # Verify the id column exists in the match table
        if match_table_id_column not in [col[1] for col in match_table_columns]:
            continue

        if not table_has_constraints(db, table_name, match_table_id_column, match_table, match_table_id_column):
            new_table_name = f"{table_name}_new"
            
            if not table_exists(db, new_table_name):
                cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
                columns = cursor.fetchall()
                column_definitions = ", ".join([
                    f"\"{col[1]}\" {col[2]}" 
                    for col in columns 
                    if col[1] != column_name
                ])
                
                create_table_sql = f"""
                    CREATE TABLE "{new_table_name}" (
                        {column_definitions},
                        "{match_table_id_column}" INTEGER,
                        FOREIGN KEY ("{match_table_id_column}") 
                        REFERENCES "{match_table}"("{match_table_id_column}")
                    );
                """
                execute_with_retry(db, create_table_sql)

                column_list = [col[1] for col in columns if col[1] != column_name]
                source_columns = ", ".join([f'"{col}"' for col in column_list])
                
                insert_sql = f"""
                    INSERT INTO "{new_table_name}" ({source_columns}, "{match_table_id_column}")
                    SELECT {source_columns},
                    (SELECT "{match_table_id_column}" 
                     FROM "{match_table}" 
                     WHERE "{match_table}"."{match_table_id_column}" = "{table_name}"."{column_name}")
                    FROM "{table_name}";
                """
                execute_with_retry(db, insert_sql)
                execute_with_retry(db, f'DROP TABLE "{table_name}";')
                execute_with_retry(db, f'ALTER TABLE "{new_table_name}" RENAME TO "{table_name}";')

    db.commit()