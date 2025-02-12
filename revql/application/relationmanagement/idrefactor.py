import sqlite3
import time
import logging
from revql.application.utils.db_connection import DatabaseConnection
from typing import Dict, Set

class RenameTracker:
    def __init__(self):
        self.renamed_columns: Dict[str, Set[str]] = {}
    
    def was_renamed(self, table: str, column: str) -> bool:
        return table in self.renamed_columns and column in self.renamed_columns[table]
    
    def track_rename(self, table: str, column: str):
        if table not in self.renamed_columns:
            self.renamed_columns[table] = set()
        self.renamed_columns[table].add(column)

def execute_with_retry(db, sql, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            db.cursor.execute(sql)
            db.commit()
            return
        except sqlite3.Error as e:
            retries += 1
            time.sleep(0.5)
            if retries >= max_retries:
                raise e

def rename_id_columns(db, tracker):
    cursor = db.cursor
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
        columns = cursor.fetchall()
        existing_columns = {col[1].lower() for col in columns}

        for column in columns:
            column_name = column[1]
            if column_name.lower() == 'id':
                new_column_name = f"{table_name}_id"
                if (new_column_name.lower() not in existing_columns and 
                    not tracker.was_renamed(table_name, new_column_name)):
                    try:
                        cursor.execute(f"""
                            ALTER TABLE "{table_name}" 
                            RENAME COLUMN "{column_name}" TO "{new_column_name}";
                        """)
                        tracker.track_rename(table_name, new_column_name)
                        logging.info(f"Renamed column '{column_name}' to '{new_column_name}' in table '{table_name}'")
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not rename column in {table_name}: {e}")

                # Ensure the new column is a primary key
                try:
                    # Create a new table with the primary key constraint
                    temp_table = f"{table_name}_temp"
                    column_defs = []
                    for col in columns:
                        if col[1].lower() == 'id':
                            column_defs.append(f'"{new_column_name}" INTEGER PRIMARY KEY AUTOINCREMENT')
                        else:
                            column_defs.append(f'"{col[1]}" {col[2]}')
                    
                    cursor.execute(f"""
                        CREATE TABLE "{temp_table}" (
                            {', '.join(column_defs)}
                        );
                    """)
                    db.commit()

                    # Copy data from the old table to the new table
                    old_columns = [col[1] for col in columns]
                    new_columns = [new_column_name if col.lower() == 'id' else col for col in old_columns]
                    cursor.execute(f"""
                        INSERT INTO "{temp_table}" ({', '.join(new_columns)})
                        SELECT {', '.join(old_columns)}
                        FROM "{table_name}";
                    """)
                    db.commit()

                    # Replace the old table with the new table
                    cursor.execute(f'DROP TABLE "{table_name}";')
                    cursor.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}";')
                    db.commit()

                    logging.info(f"Set primary key for '{new_column_name}' in table '{table_name}'")

                except sqlite3.OperationalError as e:
                    logging.warning(f"Could not set primary key in {table_name}: {e}")
                    cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}";')
                    db.commit()
    db.commit()

def rename_id_columns_and_create_relations(db_path: str, matching_info):
    db = DatabaseConnection(db_path)
    tracker = RenameTracker()

    try:
        # --- Step 1: Rename id columns & add primary keys where needed ---
        rename_id_columns(db, tracker)

        # --- Step 2: Add ProjectInformation_id column to all tables ---
        cursor = db.cursor
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            if table_name in ['ProjectInformation', 'sqlite_sequence']:
                continue

            cursor.execute(f'PRAGMA table_info("{table_name}");')
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'ProjectInformation_id' not in column_names:
                cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "ProjectInformation_id" INTEGER')

        db.commit()

        # --- Step 3: Group all detected relations (excluding self-references) by source table ---
        relations_by_table = {}
        for match in matching_info:
            if len(match) < 3:
                continue  # Skip invalid entries
            table_name, column_name, match_table = match[:3]  # Unpack the first three elements
            if table_name == match_table:
                continue
            relations_by_table.setdefault(table_name, []).append((column_name, match_table))

        # --- Step 4: For each source table, rebuild the table with all its foreign key columns ---
        for table_name, relations in relations_by_table.items():
            try:
                cursor = db.cursor
                cursor.execute(f'PRAGMA table_info("{table_name}");')
                current_columns = cursor.fetchall()
                # Identify columns to keep (skip each column that is to be replaced by a foreign key)
                skip_columns = {col_name for col_name, _ in relations}
                kept_columns = [col for col in current_columns if col[1] not in skip_columns]

                # Build new table definition
                new_col_defs = []
                table_pk = f"{table_name}_id"
                has_pk = any(col[5] == 1 for col in current_columns)
                # Ensure the primary key column is present.
                if not any(col[1].lower() == table_pk.lower() for col in kept_columns):
                    new_col_defs.append(f'"{table_pk}" INTEGER PRIMARY KEY AUTOINCREMENT')
                else:
                    for col in kept_columns:
                        if col[1].lower() == table_pk.lower():
                            new_col_defs.append(f'"{col[1]}" {col[2]} PRIMARY KEY AUTOINCREMENT')
                        else:
                            new_col_defs.append(f'"{col[1]}" {col[2]}')

                # Add ProjectInformation_id column
                if 'projectinformation_id' not in {col[1].lower() for col in current_columns}:
                    new_col_defs.append('"ProjectInformation_id" INTEGER')

                # For each relation, add a foreign key column.
                # The new FK column is named the same as the id of the reference table.
                fk_columns = []
                for column, match_table in relations:
                    fk_col = f"{match_table}_id"
                    new_col_defs.append(f'"{fk_col}" INTEGER REFERENCES "{match_table}"("{fk_col}")')
                    fk_columns.append((column, fk_col, match_table))

                # Add foreign key constraint for ProjectInformation_id
                new_col_defs.append('FOREIGN KEY("ProjectInformation_id") REFERENCES "ProjectInformation"("ProjectInformation_id")')

                new_table = f"{table_name}_new"
                create_sql = f'CREATE TABLE "{new_table}" ({", ".join(new_col_defs)});'
                execute_with_retry(db, create_sql)

                # --- Step 5: Migrate data from the old table into the new table ---
                # Prepare the list of columns to copy directly.
                kept_names = [col[1] for col in kept_columns]
                # Build the INSERT column list:
                insert_cols = kept_names + [fk for (_, fk, _) in fk_columns] + ['ProjectInformation_id']
                insert_cols_sql = ', '.join(f'"{col}"' for col in insert_cols)

                # Build the SELECT for the kept columns.
                # Alias the old table as "t" so that subqueries can reference its columns.
                select_parts = [f't."{col}"' for col in kept_names]
                # For each relation, use a correlating subquery to retrieve the matching foreign key value.
                for column, fk_col, match_table in fk_columns:
                    match_table_fk = f"{match_table}_id"
                    subquery = f"""(
                        SELECT mt."{match_table_fk}"
                        FROM "{match_table}" AS mt
                        WHERE CAST(mt."{match_table_fk}" AS TEXT) = CAST(t."{column}" AS TEXT)
                        LIMIT 1
                    )"""
                    select_parts.append(subquery)

                # Add ProjectInformation_id to the SELECT
                select_parts.append('NULL AS "ProjectInformation_id"')

                select_sql = ', '.join(select_parts)
                insert_sql = f'INSERT INTO "{new_table}" ({insert_cols_sql}) SELECT {select_sql} FROM "{table_name}" AS t;'
                logging.info(f"Executing SQL: {insert_sql}")
                execute_with_retry(db, insert_sql)

                # Replace the old table with the new one.
                execute_with_retry(db, f'DROP TABLE "{table_name}";')
                execute_with_retry(db, f'ALTER TABLE "{new_table}" RENAME TO "{table_name}";')

                db.commit()
            except Exception as e:
                logging.error(f"Error processing table {table_name}: {e}")
                db.rollback()
                continue  # Continue with the next table

        # --- Step 6: Add ProjectInformation_id foreign key to all tables without relations ---
        for table in tables:
            table_name = table[0]
            if table_name in ['ProjectInformation', 'sqlite_sequence']:
                continue

            cursor.execute(f'PRAGMA table_info("{table_name}");')
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'ProjectInformation_id' not in column_names:
                cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "ProjectInformation_id" INTEGER')
                cursor.execute(f'''
                    CREATE TABLE "{table_name}_temp" AS 
                    SELECT * FROM "{table_name}";
                ''')
                cursor.execute(f'''
                    DROP TABLE "{table_name}";
                ''')
                cursor.execute(f'''
                    CREATE TABLE "{table_name}" AS 
                    SELECT * FROM "{table_name}_temp";
                ''')
                cursor.execute(f'''
                    ALTER TABLE "{table_name}" ADD FOREIGN KEY("ProjectInformation_id") REFERENCES "ProjectInformation"("ProjectInformation_id");
                ''')
                cursor.execute(f'''
                    DROP TABLE "{table_name}_temp";
                ''')

        db.commit()

    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()