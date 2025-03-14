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
        cursor.execute(f'PRAGMA table_info("{table_name}");')
        columns = cursor.fetchall()
        existing_columns = {col[1].lower() for col in columns}

        for column in columns:
            column_name = column[1]
            if column_name.lower() == 'id':
                new_column_name = f"{table_name}_id"
                if (new_column_name.lower() not in existing_columns and 
                    not tracker.was_renamed(table_name, new_column_name)):
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
                        
                        # Fix: Make sure column names are properly quoted in SQL
                        quoted_old_columns = [f'"{col}"' for col in old_columns]
                        quoted_new_columns = [f'"{col}"' for col in new_columns]
                        
                        cursor.execute(f"""
                            INSERT INTO "{temp_table}" ({', '.join(quoted_new_columns)})
                            SELECT {', '.join(quoted_old_columns)}
                            FROM "{table_name}";
                        """)
                        db.commit()

                        # Replace the old table with the new table
                        cursor.execute(f'DROP TABLE "{table_name}";')
                        cursor.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}";')
                        db.commit()

                        tracker.track_rename(table_name, new_column_name)
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

        # --- NEW STEP: Ensure "DisciplineModel" column exists in ProjectInformation ---
        cursor = db.cursor
        cursor.execute('PRAGMA table_info("ProjectInformation");')
        cols = [col[1].lower() for col in cursor.fetchall()]
        if 'disciplinemodel' not in cols:
            cursor.execute('ALTER TABLE "ProjectInformation" ADD COLUMN "DisciplineModel" TEXT')
            logging.info('Added column "DisciplineModel" to table "ProjectInformation".')
        db.commit()

        # --- Step 2: Add ProjectInformation_id column to all tables ---
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
                # Drop existing temporary table if it exists
                new_table_name = f"{table_name}_new"
                cursor.execute(f'DROP TABLE IF EXISTS "{new_table_name}"')

                # Create a new table with the foreign key constraint
                column_defs = [f'"{col[1]}" {col[2]}' for col in columns]
                column_defs.append('"ProjectInformation_id" INTEGER')
                column_defs.append('FOREIGN KEY("ProjectInformation_id") REFERENCES "ProjectInformation"("ProjectInformation_id")')
                create_sql = f'CREATE TABLE "{new_table_name}" ({", ".join(column_defs)})'
                cursor.execute(create_sql)

                # Copy data from the old table to the new table - FIX HERE
                quoted_columns = [f'"{col[1]}"' for col in columns]
                
                # Fix: Separate column names from SELECT statement with NULL value for ProjectInformation_id
                insert_sql = f'''
                    INSERT INTO "{new_table_name}" ({", ".join(quoted_columns)}, "ProjectInformation_id")
                    SELECT {", ".join(quoted_columns)}, NULL
                    FROM "{table_name}"
                '''
                cursor.execute(insert_sql)

                # Replace the old table with the new table
                cursor.execute(f'DROP TABLE "{table_name}"')
                cursor.execute(f'ALTER TABLE "{new_table_name}" RENAME TO "{table_name}"')

        db.commit()

        # --- Step 3: Group all detected relations (excluding self-references) by source table ---
        relations_by_table = {}
        for match in matching_info:
            if len(match) < 3:
                continue
            table_name, column_name, match_table = match[:3]
            if table_name == match_table:
                continue
            relations_by_table.setdefault(table_name, []).append((column_name, match_table))

        # --- Step 4: Process each table to add foreign key constraints ---
        for table in tables:
            table_name = table[0]
            if table_name in ['ProjectInformation', 'sqlite_sequence']:
                continue

            try:
                # Get current table structure
                cursor.execute(f'PRAGMA table_info("{table_name}");')
                columns = cursor.fetchall()
                column_names = [col[1].lower() for col in columns]
                
                # Get columns that will become foreign keys
                skip_columns = set()
                if table_name in relations_by_table:
                    skip_columns = {col[0] for col, _ in relations_by_table[table_name]}
                
                # Build new table definition
                new_col_defs = []
                
                # Add table_name_id as primary key first
                new_col_defs.append(f'"{table_name}_id" INTEGER PRIMARY KEY AUTOINCREMENT')
                
                # Add Id column to match table_name_id
                new_col_defs.append('"Id" INTEGER')
                
                # Process existing columns
                for col in columns:
                    if (col[1].lower() != 'projectinformation_id' and 
                        col[1] not in skip_columns and
                        col[1].lower() != f"{table_name}_id".lower() and
                        col[1].lower() != 'id'):
                        new_col_defs.append(f'"{col[1]}" {col[2]}')
                
                # Add ProjectInformation_id column
                new_col_defs.append('"ProjectInformation_id" INTEGER')
                
                # Add foreign key columns - with unique name check
                fk_columns = []
                existing_fk_names = set(col_name.lower() for col_name in column_names)
                
                if table_name in relations_by_table:
                    for orig_column, match_table in relations_by_table[table_name]:
                        base_fk_col = f"{match_table}_id"
                        fk_col = base_fk_col
                        
                        # Check if column name already exists
                        counter = 1
                        while fk_col.lower() in existing_fk_names:
                            fk_col = f"{match_table}{counter}_id"
                            counter += 1
                        
                        # Add the unique FK column
                        new_col_defs.append(f'"{fk_col}" INTEGER')
                        fk_columns.append((orig_column, fk_col, match_table))
                        existing_fk_names.add(fk_col.lower())
                
                # Add foreign key constraints
                new_col_defs.append('FOREIGN KEY("ProjectInformation_id") REFERENCES "ProjectInformation"("ProjectInformation_id")')
                for _, fk_col, match_table in fk_columns:
                    new_col_defs.append(f'FOREIGN KEY("{fk_col}") REFERENCES "{match_table}"("{match_table}_id")')
                
                # Drop existing temporary table if it exists
                new_table = f"{table_name}_new"
                cursor.execute(f'DROP TABLE IF EXISTS "{new_table}"')

                # Create new table
                create_sql = f'CREATE TABLE "{new_table}" ({", ".join(new_col_defs)})'
                cursor.execute(create_sql)
                
                # Prepare columns for data migration
                kept_columns = [col[1] for col in columns 
                              if col[1].lower() != 'projectinformation_id' 
                              and col[1] not in skip_columns
                              and col[1].lower() != 'id']
                
                # Build SELECT statement
                select_parts = []
                # Add kept columns
                select_parts.extend(f't."{col}"' for col in kept_columns)
                
                # Add foreign key lookups
                for orig_column, fk_col, match_table in fk_columns:
                    select_parts.append(f'''(
                        SELECT m."{match_table}_id" 
                        FROM "{match_table}" m 
                        WHERE CAST(m."{match_table}_id" AS TEXT) = CAST(t."{orig_column}" AS TEXT) 
                        LIMIT 1
                    ) AS "{fk_col}"''')
                
                # Fix: Check if ProjectInformation_id column exists before using it
                cursor.execute(f'PRAGMA table_info("{table_name}");')
                has_project_info_id = any(col[1].lower() == 'projectinformation_id' for col in cursor.fetchall())
                
                if has_project_info_id:
                    # Add ProjectInformation_id
                    select_parts.append('t."ProjectInformation_id"')
                else:
                    # Use NULL if the column doesn't exist
                    select_parts.append('NULL AS "ProjectInformation_id"')
                
                # Build INSERT columns list
                insert_cols = []
                insert_cols.extend(f'"{col}"' for col in kept_columns)
                insert_cols.extend(f'"{fk[1]}"' for fk in fk_columns)
                insert_cols.append('"ProjectInformation_id"')
                
                # Execute INSERT
                insert_sql = f'''
                    INSERT INTO "{new_table}" ({", ".join(insert_cols)})
                    SELECT {", ".join(select_parts)}
                    FROM "{table_name}" t
                '''
                try:
                    cursor.execute(insert_sql)
                except sqlite3.OperationalError as e:
                    logging.warning(f"Error inserting data into {new_table}: {e}")
                    # Try a simpler approach
                    kept_cols_quoted = [f'"{col}"' for col in kept_columns]
                    cursor.execute(f'''
                        INSERT INTO "{new_table}" ({", ".join(kept_cols_quoted)}, "ProjectInformation_id")
                        SELECT {", ".join(kept_cols_quoted)}, NULL
                        FROM "{table_name}"
                    ''')
                    # Set foreign keys to NULL for now
                    for _, fk_col, _ in fk_columns:
                        cursor.execute(f'UPDATE "{new_table}" SET "{fk_col}" = NULL')
                
                # Update the Id column to match tablename_id
                cursor.execute(f'UPDATE "{new_table}" SET "Id" = "{table_name}_id"')
                
                # Replace old table with new one
                cursor.execute(f'DROP TABLE "{table_name}"')
                cursor.execute(f'ALTER TABLE "{new_table}" RENAME TO "{table_name}"')
                
                db.commit()
                
            except sqlite3.OperationalError as e:
                logging.warning(f"Error processing table {table_name}: {e}")
                cursor.execute(f'DROP TABLE IF EXISTS "{table_name}_new"')
                db.commit()
                continue

    except Exception as e:
        db.rollback()
        logging.error(f"Database operation failed: {str(e)}")
        raise e
    finally:
        db.close()