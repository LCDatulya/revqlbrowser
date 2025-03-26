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
    """
    Rename the 'id' column in all tables to '<tablename>_id' without creating duplicate columns.
    """
    cursor = db.cursor
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        if table_name in ['sqlite_sequence', 'ProjectInformation']:
            continue
            
        cursor.execute(f'PRAGMA table_info("{table_name}");')
        columns = cursor.fetchall()
        existing_columns = {col[1].lower() for col in columns}
        
        # Check for existing tablename_id column
        tablename_id_exists = f"{table_name.lower()}_id" in existing_columns
        id_column_exists = 'id' in existing_columns
        
        # Skip if there's no 'id' column or if tablename_id already exists
        if not id_column_exists or tablename_id_exists:
            continue
            
        # Create a new table with the renamed column
        temp_table = f"{table_name}_temp"
        column_defs = []
        
        for col in columns:
            col_name = col[1]
            col_type = col[2]
            
            if col_name.lower() == 'id':
                new_col_name = f"{table_name}_id"
                column_defs.append(f'"{new_col_name}" {col_type} PRIMARY KEY')
            else:
                column_defs.append(f'"{col_name}" {col_type}')
        
        try:
            # Create the temporary table
            cursor.execute(f'''
                CREATE TABLE "{temp_table}" (
                    {', '.join(column_defs)}
                );
            ''')
            
            # Copy data from the old table to the new table
            old_columns = [col[1] for col in columns]
            new_columns = [f"{table_name}_id" if col.lower() == 'id' else col for col in old_columns]
            
            # Ensure column names are properly quoted
            quoted_old_columns = [f'"{col}"' for col in old_columns]
            quoted_new_columns = [f'"{col}"' for col in new_columns]
            
            cursor.execute(f'''
                INSERT INTO "{temp_table}" ({', '.join(quoted_new_columns)})
                SELECT {', '.join(quoted_old_columns)}
                FROM "{table_name}";
            ''')
            
            # Replace the old table with the new table
            cursor.execute(f'DROP TABLE "{table_name}";')
            cursor.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}";')
            
            db.commit()
            tracker.track_rename(table_name, f"{table_name}_id")
            logging.info(f"Renamed 'id' to '{table_name}_id' in table '{table_name}'")
            
        except sqlite3.OperationalError as e:
            logging.warning(f"Could not rename 'id' in table '{table_name}': {e}")
            cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}";')
            db.commit()

def rename_id_columns_and_create_relations(db_path: str, matching_info):
    db = DatabaseConnection(db_path)
    tracker = RenameTracker()

    try:
        # Step 1: Rename id columns & add primary keys where needed
        rename_id_columns(db, tracker)
        
        # Step 2: Ensure "DisciplineModel" column exists in ProjectInformation
        cursor = db.cursor
        cursor.execute('PRAGMA table_info("ProjectInformation");')
        cols = [col[1].lower() for col in cursor.fetchall()]
        if 'disciplinemodel' not in cols:
            cursor.execute('ALTER TABLE "ProjectInformation" ADD COLUMN "DisciplineModel" TEXT')
            logging.info('Added column "DisciplineModel" to table "ProjectInformation".')
        db.commit()
        
        # Step 3: Add ProjectInformation_id to tables that don't have it
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            if table_name in ['ProjectInformation', 'sqlite_sequence']:
                continue
                
            cursor.execute(f'PRAGMA table_info("{table_name}");')
            columns = cursor.fetchall()
            column_names = [col[1].lower() for col in columns]
            
            # Only add ProjectInformation_id if it doesn't exist
            if 'projectinformation_id' not in column_names:
                try:
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "ProjectInformation_id" INTEGER')
                    logging.info(f"Added ProjectInformation_id to table {table_name}")
                except sqlite3.OperationalError as e:
                    logging.warning(f"Could not add ProjectInformation_id to {table_name}: {e}")
        
        db.commit()
        
        # Step 4: Group detected relations by source table
        relations_by_table = {}
        for match in matching_info:
            if len(match) < 3:
                continue
            table_name, column_name, match_table = match[:3]
            if table_name == match_table:
                continue
            relations_by_table.setdefault(table_name, []).append((column_name, match_table))
        
        # Step 5: Create foreign key relationships
        for table in tables:
            table_name = table[0]
            if table_name in ['ProjectInformation', 'sqlite_sequence'] or table_name not in relations_by_table:
                continue
                
            try:
                # Get current table structure
                cursor.execute(f'PRAGMA table_info("{table_name}");')
                columns = cursor.fetchall()
                column_dict = {col[1].lower(): col for col in columns}
                
                # Create new table with foreign keys
                temp_table = f"{table_name}_temp"
                cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
                
                # Build column definitions for new table
                col_defs = []
                
                # Add original columns first (except those that will be replaced with FKs)
                for col in columns:
                    col_name = col[1]
                    skip_column = False
                    
                    # Skip columns that will be replaced with foreign keys
                    for orig_col, _ in relations_by_table[table_name]:
                        if col_name.lower() == orig_col.lower():
                            skip_column = True
                            break
                            
                    if not skip_column:
                        col_defs.append(f'"{col_name}" {col[2]}')
                
                # Add foreign key columns
                fk_defs = []
                for orig_column, match_table in relations_by_table[table_name]:
                    fk_col = f"{match_table}_id"
                    
                    # Only add if not already in the table
                    if fk_col.lower() not in column_dict:
                        col_defs.append(f'"{fk_col}" INTEGER')
                        fk_defs.append(f'FOREIGN KEY("{fk_col}") REFERENCES "{match_table}"("{match_table}_id")')
                
                # Add ProjectInformation foreign key
                if 'projectinformation_id' in column_dict:
                    fk_defs.append('FOREIGN KEY("ProjectInformation_id") REFERENCES "ProjectInformation"("ProjectInformation_id")')
                
                # Create the new table
                create_sql = f'''
                    CREATE TABLE "{temp_table}" (
                        {", ".join(col_defs + fk_defs)}
                    )
                '''
                cursor.execute(create_sql)
                
                # Copy data from old table to new table
                # Prepare column lists for old and new tables
                old_cols = []
                new_cols = []
                
                for col in columns:
                    col_name = col[1]
                    skip_column = False
                    
                    # Skip columns that will be replaced with foreign keys
                    for orig_col, _ in relations_by_table[table_name]:
                        if col_name.lower() == orig_col.lower():
                            skip_column = True
                            break
                            
                    if not skip_column:
                        old_cols.append(f'"{col_name}"')
                        new_cols.append(f'"{col_name}"')
                
                # Add foreign key lookups
                for orig_column, match_table in relations_by_table[table_name]:
                    fk_col = f"{match_table}_id"
                    if fk_col.lower() not in column_dict:
                        # Add FK lookup to the SELECT part
                        new_cols.append(f'"{fk_col}"')
                        old_cols.append(f'''(
                            SELECT m."{match_table}_id" 
                            FROM "{match_table}" m 
                            WHERE CAST(m."{match_table}_id" AS TEXT) = CAST(t."{orig_column}" AS TEXT) 
                            LIMIT 1
                        )''')
                
                # Insert data
                insert_sql = f'''
                    INSERT INTO "{temp_table}" ({", ".join(new_cols)})
                    SELECT {", ".join(old_cols)}
                    FROM "{table_name}" t
                '''
                
                try:
                    cursor.execute(insert_sql)
                except sqlite3.OperationalError as e:
                    logging.warning(f"Error inserting data for {table_name}: {e}")
                    # Fall back to simple copy
                    cursor.execute(f'''
                        INSERT INTO "{temp_table}" ({", ".join(new_cols)})
                        SELECT {", ".join([col for col in old_cols if "SELECT" not in col])}
                        FROM "{table_name}"
                    ''')
                
                # Replace old table with new table
                cursor.execute(f'DROP TABLE "{table_name}"')
                cursor.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')
                db.commit()
                
                logging.info(f"Created foreign key constraints for {table_name}")
                
            except sqlite3.OperationalError as e:
                logging.warning(f"Error creating foreign keys for {table_name}: {e}")
                cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
                db.commit()
    
    except Exception as e:
        db.rollback()
        logging.error(f"Error in rename_id_columns_and_create_relations: {e}")
        raise
    finally:
        db.close()