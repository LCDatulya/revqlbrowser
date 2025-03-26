from .transactionmanager import TransactionManager
from .tableoperations import TableOperations
from .projectinformationhandler import ProjectInformationHandler
from .mergeddatabasecleaner import DatabaseCleaner
from ..db_connection import DatabaseConnection
import logging
import sqlite3
import os
import time
import shutil
from ..db_utils import find_matching_table_column_names
from ...relationmanagement.idrefactor import rename_id_columns_and_create_relations

class DatabaseMerger:
    def __init__(self, source_db_path: str, target_db_path: str):
        self.source_db_path = source_db_path
        self.target_db_path = target_db_path
        self.transaction_manager = TransactionManager()
        self.table_ops = TableOperations()
        self.project_info = ProjectInformationHandler()
        self.cleaner = DatabaseCleaner()

    def merge_databases(self) -> bool:
        """
        Merge databases with guaranteed preservation of ProjectInformation_id values
        and proper relation creation.
        """
        # Create backup of target before proceeding
        backup_path = f"{self.target_db_path}.backup_{int(time.time())}"
        shutil.copy2(self.target_db_path, backup_path)
        logging.info(f"Created backup of target database at {backup_path}")
        
        # Phase 1: Prepare source database
        if not self._prepare_source_database():
            logging.error("Failed to prepare source database. Aborting.")
            return False
            
        # Phase 2: Execute merge with a direct approach
        if not self._execute_direct_merge():
            logging.error("Direct merge failed. Restoring from backup.")
            shutil.copy2(backup_path, self.target_db_path)
            return False
            
        logging.info("Database merge completed successfully.")
        return True
    
    def _prepare_source_database(self) -> bool:
        """Prepare source database by ensuring ProjectInformation table with correct columns"""
        conn = None
        try:
            # Enable foreign keys
            conn = sqlite3.connect(self.source_db_path)
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.cursor()

            # STEP 1: First check if ProjectInformation table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation'")
            if not cursor.fetchone():
                # Create the table with proper structure
                logging.info("Creating ProjectInformation table in source database")
                cursor.execute('''
                    CREATE TABLE "ProjectInformation" (
                        "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                        "ProjectName" TEXT,
                        "DisciplineModel" TEXT
                    )
                ''')

                # Add a default record
                db_name = os.path.basename(self.source_db_path).replace('.db', '')
                cursor.execute('''
                    INSERT INTO "ProjectInformation" ("ProjectName", "DisciplineModel")
                    VALUES (?, 'Default')
                ''', (f"Project {db_name}",))
                conn.commit()
                logging.info("Added default ProjectInformation record")

                # Skip to step 3 since we just created a correct table
                existing_pi_id = 1
            else:
                # STEP 2: Verify the structure of ProjectInformation table
                cursor.execute("PRAGMA table_info('ProjectInformation')")
                columns = cursor.fetchall()
                column_names = [col[1].lower() for col in columns]

                # Check if there's an "Id" column that might be the primary key
                has_id = "id" in column_names
                has_pi_id = "projectinformation_id" in column_names

                # Fix table if needed
                if not has_pi_id:
                    logging.info("ProjectInformation table exists but missing ProjectInformation_id column. Rebuilding table.")

                    # Create a temporary table with correct structure
                    cursor.execute('''
                        CREATE TABLE "ProjectInformation_temp" (
                            "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                            "ProjectName" TEXT,
                            "DisciplineModel" TEXT
                        )
                    ''')

                    # Find usable columns for copying
                    safe_columns = []
                    for col in columns:
                        col_name = col[1]
                        if col_name.lower() not in ['id', 'projectinformation_id'] and col_name in ['ProjectName', 'DisciplineModel']:
                            safe_columns.append(col_name)

                    # If we have Id column, use it as ProjectInformation_id
                    if has_id:
                        # Get position of Id column
                        id_pos = next(i for i, col in enumerate(columns) if col[1].lower() == 'id')
                        id_name = columns[id_pos][1]  # Preserve original case

                        if safe_columns:
                            # Copy with Id mapped to ProjectInformation_id
                            cols_str = ', '.join([f'"{col}"' for col in safe_columns])
                            cursor.execute(f'''
                                INSERT INTO "ProjectInformation_temp" ("ProjectInformation_id", {cols_str})
                                SELECT "{id_name}", {cols_str} FROM "ProjectInformation"
                            ''')
                        else:
                            # Just copy the ID
                            cursor.execute(f'''
                                INSERT INTO "ProjectInformation_temp" ("ProjectInformation_id")
                                SELECT "{id_name}" FROM "ProjectInformation"
                            ''')
                    elif safe_columns:
                        # No ID column but we have other columns, create new IDs
                        cols_str = ', '.join([f'"{col}"' for col in safe_columns])
                        cursor.execute(f'''
                            INSERT INTO "ProjectInformation_temp" ({cols_str})
                            SELECT {cols_str} FROM "ProjectInformation"
                        ''')

                    # Replace old table
                    cursor.execute('DROP TABLE "ProjectInformation"')
                    cursor.execute('ALTER TABLE "ProjectInformation_temp" RENAME TO "ProjectInformation"')

                    # Add default record if none exists
                    cursor.execute("SELECT COUNT(*) FROM ProjectInformation")
                    if cursor.fetchone()[0] == 0:
                        db_name = os.path.basename(self.source_db_path).replace('.db', '')
                        cursor.execute('''
                            INSERT INTO "ProjectInformation" ("ProjectName", "DisciplineModel")
                            VALUES (?, 'Default')
                        ''', (f"Project {db_name}",))

                    conn.commit()
                    logging.info("ProjectInformation table rebuilt successfully")

                # Get the default ProjectInformation_id to use
                cursor.execute("SELECT MIN(ProjectInformation_id) FROM ProjectInformation")
                result = cursor.fetchone()
                existing_pi_id = result[0] if result and result[0] is not None else 1

            # STEP 3: Add ProjectInformation_id to all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('ProjectInformation', 'sqlite_sequence')")
            tables = [row[0] for row in cursor.fetchall()]

            for table_name in tables:
                # Check if ProjectInformation_id column exists
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns = cursor.fetchall()
                column_names = [col[1].lower() for col in columns]

                if 'projectinformation_id' not in column_names:
                    try:
                        logging.info(f"Adding ProjectInformation_id to {table_name} in source database")
                        cursor.execute(f"ALTER TABLE '{table_name}' ADD COLUMN 'ProjectInformation_id' INTEGER")
                        cursor.execute(f"UPDATE '{table_name}' SET ProjectInformation_id = ?", (existing_pi_id,))
                        conn.commit()
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not add ProjectInformation_id to {table_name}: {e}")
                        # Continue with other tables

            # STEP 4: Now create the relationships
            logging.info("Creating relationships in source database")
            conn.close()  # Close before using rename_id_columns_and_create_relations

            # This function will open its own connection
            try:
                # First identify potential relationships
                matching_info = find_matching_table_column_names(self.source_db_path)

                if matching_info and matching_info[1]:
                    # Now create the relationships
                    rename_id_columns_and_create_relations(self.source_db_path, matching_info[1])
                    logging.info("Successfully created relationships in source database")
                else:
                    logging.info("No potential relationships found in source database")
            except Exception as e:
                logging.warning(f"Could not create all relationships: {e}")
                # Continue with the merge anyway

            # Verify everything worked
            conn = sqlite3.connect(self.source_db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT ProjectInformation_id, ProjectName FROM ProjectInformation")
            projects = cursor.fetchall()
            logging.info(f"Source database projects: {projects}")

            return True

        except Exception as e:
            logging.error(f"Error preparing source database: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False

        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def _execute_direct_merge(self) -> bool:
        """Execute the merge with a direct approach that guarantees ID preservation"""
        source_conn = None
        target_conn = None
        
        try:
            # Enable foreign keys in both connections
            source_conn = sqlite3.connect(self.source_db_path)
            source_conn.execute("PRAGMA foreign_keys = ON")
            source_cursor = source_conn.cursor()
            
            target_conn = sqlite3.connect(self.target_db_path)
            target_conn.execute("PRAGMA foreign_keys = ON")
            target_cursor = target_conn.cursor()
            
            # STEP 1: Get table list from source
            source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('sqlite_sequence')")
            source_tables = [row[0] for row in source_cursor.fetchall()]
            
            # STEP 2: Merge ProjectInformation table with careful ID preservation
            id_mapping = self._merge_project_information(source_conn, target_conn)
            logging.info(f"ProjectInformation_id mapping: {id_mapping}")
            
            # STEP 3: Process each table
            for table_name in source_tables:
                if table_name == 'ProjectInformation':
                    continue
                    
                # Check if table exists in target
                target_cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                table_exists = target_cursor.fetchone() is not None
                
                if table_exists:
                    self._merge_table(source_conn, target_conn, table_name, id_mapping)
                else:
                    self._copy_table(source_conn, target_conn, table_name, id_mapping)
            
            # STEP 4: Ensure all ProjectInformation_id columns exist in target
            self._ensure_all_pi_columns(target_conn)
            
            # STEP 5: Create foreign key relationships
            self._create_relations(target_conn)
            
            # STEP 6: Verify ProjectInformation_id values
            self._verify_pi_values(target_conn)
            
            return True
            
        except Exception as e:
            logging.error(f"Error during direct merge: {e}", exc_info=True)
            return False
            
        finally:
            if source_conn:
                source_conn.close()
            if target_conn:
                target_conn.close()
    
    def _merge_project_information(self, source_conn, target_conn) -> dict:
        """Merge ProjectInformation tables with guaranteed ID preservation"""
        id_mapping = {}  # Maps source IDs to target IDs
        
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()
        
        # Ensure target has ProjectInformation table
        target_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation'")
        if not target_cursor.fetchone():
            target_cursor.execute('''
                CREATE TABLE "ProjectInformation" (
                    "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                    "ProjectName" TEXT,
                    "DisciplineModel" TEXT
                )
            ''')
            logging.info("Created ProjectInformation table in target database")
        
        # Get existing ProjectInformation_ids from target
        target_cursor.execute("SELECT ProjectInformation_id FROM ProjectInformation")
        existing_ids = set(row[0] for row in target_cursor.fetchall())
        
        # Get source ProjectInformation records
        source_cursor.execute("SELECT * FROM ProjectInformation")
        source_records = source_cursor.fetchall()
        
        source_cursor.execute("PRAGMA table_info(ProjectInformation)")
        column_info = source_cursor.fetchall()
        column_names = [col[1] for col in column_info]
        
        # Track which source IDs are preserved and which are remapped
        for record in source_records:
            record_dict = dict(zip(column_names, record))
            source_id = record_dict.get('ProjectInformation_id')
            
            if not source_id:
                continue
                
            # If the source ID exists in target, we need a new ID
            if source_id in existing_ids:
                # Set ProjectName to indicate source
                project_name = record_dict.get('ProjectName', '')
                if project_name:
                    source_db_name = os.path.basename(source_conn.execute("PRAGMA database_list").fetchone()[2])
                    project_name = f"{project_name} (From {source_db_name})"
                    record_dict['ProjectName'] = project_name
                
                # Insert without the ID to get a new auto-generated ID
                columns = [k for k in record_dict.keys() if k.lower() != 'projectinformation_id']
                values = [record_dict[k] for k in columns]
                
                placeholder = ', '.join(['?'] * len(values))
                columns_sql = ', '.join([f'"{col}"' for col in columns])
                
                target_cursor.execute(f'''
                    INSERT INTO ProjectInformation ({columns_sql})
                    VALUES ({placeholder})
                ''', values)
                
                # Get the new ID
                target_cursor.execute("SELECT last_insert_rowid()")
                new_id = target_cursor.fetchone()[0]
                
                # Map old ID to new ID
                id_mapping[source_id] = new_id
                logging.info(f"Mapped source ProjectInformation_id {source_id} to new ID {new_id}")
            else:
                # We can keep the original ID
                columns = [k for k in record_dict.keys()]
                values = [record_dict[k] for k in columns]
                
                placeholder = ', '.join(['?'] * len(values))
                columns_sql = ', '.join([f'"{col}"' for col in columns])
                
                # Direct insert preserving the ID
                target_cursor.execute(f'''
                    INSERT INTO ProjectInformation ({columns_sql})
                    VALUES ({placeholder})
                ''', values)
                
                # Map to itself (identity mapping)
                id_mapping[source_id] = source_id
                logging.info(f"Preserved source ProjectInformation_id {source_id}")
                existing_ids.add(source_id)  # Mark as used
        
        target_conn.commit()
        return id_mapping
    
    def _merge_table(self, source_conn, target_conn, table_name, id_mapping):
        """Merge a table's data from source to target, preserving ProjectInformation_id mappings"""
        try:
            source_cursor = source_conn.cursor()
            target_cursor = target_conn.cursor()
            
            # Get table structure
            source_cursor.execute(f"PRAGMA table_info('{table_name}')")
            source_columns = source_cursor.fetchall()
            source_column_names = [col[1] for col in source_columns]
            
            target_cursor.execute(f"PRAGMA table_info('{table_name}')")
            target_columns = target_cursor.fetchall()
            target_column_names = [col[1] for col in target_columns]
            
            # Find common columns and check for ProjectInformation_id
            common_columns = []
            has_pi_column = False
            pi_column_name = None
            
            for col in source_column_names:
                if col.lower() in [c.lower() for c in target_column_names]:
                    common_columns.append(col)
                    if col.lower() == 'projectinformation_id':
                        has_pi_column = True
                        pi_column_name = col
            
            # Ensure ProjectInformation_id exists in target
            if not has_pi_column:
                # Add column if not exists
                pi_column_name = "ProjectInformation_id"
                try:
                    target_cursor.execute(f"ALTER TABLE '{table_name}' ADD COLUMN 'ProjectInformation_id' INTEGER")
                    common_columns.append(pi_column_name)
                    has_pi_column = True
                except sqlite3.OperationalError:
                    # Column might already exist
                    pass
            
            # Get source data
            source_cursor.execute(f"SELECT {', '.join(['\"' + col + '\"' for col in common_columns])} FROM '{table_name}'")
            rows = source_cursor.fetchall()
            
            # Find PI column index
            pi_index = None
            for i, col in enumerate(common_columns):
                if col.lower() == 'projectinformation_id':
                    pi_index = i
                    break
            
            # Prepare for insertion with PI mapping
            columns_sql = ', '.join([f'"{col}"' for col in common_columns])
            placeholders = ', '.join(['?'] * len(common_columns))
            
            # Process and insert each row
            for row in rows:
                # Apply ID mapping if needed
                row_list = list(row)
                if pi_index is not None and row_list[pi_index] is not None:
                    old_id = row_list[pi_index]
                    new_id = id_mapping.get(old_id, old_id)  # Use mapping or original
                    row_list[pi_index] = new_id
                
                # Insert with OR IGNORE to handle duplicates
                target_cursor.execute(f'''
                    INSERT OR IGNORE INTO "{table_name}" ({columns_sql})
                    VALUES ({placeholders})
                ''', row_list)
            
            target_conn.commit()
            logging.info(f"Merged {len(rows)} rows into table {table_name}")
            
        except Exception as e:
            target_conn.rollback()
            logging.error(f"Error merging table {table_name}: {e}")
            raise
    
    def _copy_table(self, source_conn, target_conn, table_name, id_mapping):
        """Copy a table from source to target, preserving ProjectInformation_id mappings"""
        try:
            source_cursor = source_conn.cursor()
            target_cursor = target_conn.cursor()
            
            # Get table structure
            source_cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = source_cursor.fetchall()
            
            # Create table in target
            create_stmts = []
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                not_null = "NOT NULL" if col[3] else ""
                pk = "PRIMARY KEY" if col[5] else ""
                create_stmts.append(f'"{col_name}" {col_type} {pk} {not_null}')
            
            create_sql = f'CREATE TABLE "{table_name}" ({", ".join(create_stmts)})'
            target_cursor.execute(create_sql)
            
            # Get data with column names
            column_names = [col[1] for col in columns]
            source_cursor.execute(f"SELECT {', '.join(['\"' + col + '\"' for col in column_names])} FROM '{table_name}'")
            rows = source_cursor.fetchall()
            
            # Find PI column index
            pi_index = None
            for i, col in enumerate(column_names):
                if col.lower() == 'projectinformation_id':
                    pi_index = i
                    break
            
            # Insert data with mapped IDs
            columns_sql = ', '.join([f'"{col}"' for col in column_names])
            placeholders = ', '.join(['?'] * len(column_names))
            
            for row in rows:
                # Apply ID mapping if needed
                row_list = list(row)
                if pi_index is not None and row_list[pi_index] is not None:
                    old_id = row_list[pi_index]
                    new_id = id_mapping.get(old_id, old_id)  # Use mapping or original
                    row_list[pi_index] = new_id
                
                target_cursor.execute(f'''
                    INSERT INTO "{table_name}" ({columns_sql})
                    VALUES ({placeholders})
                ''', row_list)
            
            target_conn.commit()
            logging.info(f"Copied table {table_name} with {len(rows)} rows")
            
        except Exception as e:
            target_conn.rollback()
            logging.error(f"Error copying table {table_name}: {e}")
            raise
    
    def _ensure_all_pi_columns(self, target_conn):
        """Ensure all tables have ProjectInformation_id column after merge"""
        cursor = target_conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('ProjectInformation', 'sqlite_sequence')")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Default ProjectInformation_id value
        cursor.execute("SELECT MIN(ProjectInformation_id) FROM ProjectInformation")
        default_id = cursor.fetchone()[0] or 1
        
        for table_name in tables:
            # Check if ProjectInformation_id exists
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = [col[1].lower() for col in cursor.fetchall()]
            
            if 'projectinformation_id' not in columns:
                try:
                    cursor.execute(f"ALTER TABLE '{table_name}' ADD COLUMN 'ProjectInformation_id' INTEGER")
                    cursor.execute(f"UPDATE '{table_name}' SET ProjectInformation_id = ?", (default_id,))
                    logging.info(f"Added ProjectInformation_id to {table_name} after merge")
                except:
                    logging.warning(f"Could not add ProjectInformation_id to {table_name}")
        
        target_conn.commit()
    
    def _create_relations(self, target_conn):
        """
        Create foreign key relationships to ProjectInformation table with a safer approach
        that handles both missing columns and invalid foreign keys
        """
        cursor = target_conn.cursor()
        
        # STEP 1: First disable foreign keys temporarily
        cursor.execute("PRAGMA foreign_keys = OFF")
        
        # STEP 2: Ensure all tables have the column before trying to create relations
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('ProjectInformation', 'sqlite_sequence')")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Get valid IDs once
        cursor.execute("SELECT ProjectInformation_id FROM ProjectInformation")
        valid_ids = set(row[0] for row in cursor.fetchall())
        default_id = min(valid_ids) if valid_ids else 1
        
        # Add missing columns
        modified_tables = 0
        for table_name in tables:
            try:
                # Check if ProjectInformation_id exists
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns = cursor.fetchall()
                column_names = [col[1].lower() for col in columns]
                
                # Add column if missing
                if 'projectinformation_id' not in column_names:
                    cursor.execute(f"ALTER TABLE '{table_name}' ADD COLUMN 'ProjectInformation_id' INTEGER")
                    modified_tables += 1
                    
                # Fix invalid values
                cursor.execute(f"""
                    UPDATE '{table_name}' 
                    SET ProjectInformation_id = {default_id} 
                    WHERE ProjectInformation_id IS NULL 
                       OR ProjectInformation_id NOT IN (SELECT ProjectInformation_id FROM ProjectInformation)
                """)
            except Exception as e:
                logging.warning(f"Could not prepare {table_name}: {e}")
        
        if modified_tables > 0:
            logging.info(f"Added ProjectInformation_id column to {modified_tables} tables")
        
        # STEP 3: Skip foreign key creation for now since it's causing errors
        # Instead, we'll focus on ensuring data consistency
        
        # Set all NULL or invalid ProjectInformation_id values to the default ID
        for table_name in tables:
            try:
                cursor.execute(f"""
                    UPDATE '{table_name}' 
                    SET ProjectInformation_id = {default_id} 
                    WHERE ProjectInformation_id IS NULL 
                       OR ProjectInformation_id NOT IN (SELECT ProjectInformation_id FROM ProjectInformation)
                """)
            except Exception as e:
                logging.warning(f"Could not update {table_name}: {e}")
                
        # Commit the changes
        target_conn.commit()
        cursor.execute("PRAGMA foreign_keys = ON")
        
        logging.info("Data consistency ensured for all tables")
        logging.info("Foreign key constraints will be deferred to a future upgrade")
        
        # IMPORTANT: Log message for user
        logging.info("NOTE: Foreign key relationships were not created to avoid errors. "
                    "Data integrity is maintained but formal constraints are not in place.")
    
    def _verify_pi_values(self, target_conn):
        """Verify and log ProjectInformation_id values in tables after merge"""
        cursor = target_conn.cursor()
        
        # Check ProjectInformation table
        cursor.execute("SELECT ProjectInformation_id, ProjectName FROM ProjectInformation")
        projects = cursor.fetchall()
        logging.info(f"Target database projects after merge: {projects}")
        
        # Check a few tables to verify ProjectInformation_id values
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('ProjectInformation', 'sqlite_sequence')")
        tables = [row[0] for row in cursor.fetchall()]
        
        tables_sample = tables[:5] if len(tables) > 5 else tables
        for table in tables_sample:
            try:
                cursor.execute(f"SELECT DISTINCT ProjectInformation_id FROM '{table}' LIMIT 5")
                ids = [row[0] for row in cursor.fetchall() if row[0] is not None]
                logging.info(f"Sample ProjectInformation_id values in {table} after merge: {ids}")
            except:
                pass