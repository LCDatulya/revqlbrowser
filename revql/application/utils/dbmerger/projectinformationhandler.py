from typing import Dict, Optional
from ..db_connection import DatabaseConnection
import sqlite3
import logging
import os

class ProjectInformationHandler:
    @staticmethod
    def merge_project_information(source_db: DatabaseConnection, target_db: DatabaseConnection) -> Dict[int, int]:
        """Merge ProjectInformation records, PRESERVING source project IDs"""
        id_mapping = {}
        try:
            # First, analyze existing IDs in both databases to detect conflicts
            source_db.cursor.execute('SELECT ProjectInformation_id FROM ProjectInformation')
            source_ids = {row[0] for row in source_db.cursor.fetchall() if row[0] is not None}
            
            target_db.cursor.execute('SELECT ProjectInformation_id FROM ProjectInformation')
            target_ids = {row[0] for row in target_db.cursor.fetchall() if row[0] is not None}
            
            # Find conflicting IDs
            conflicting_ids = source_ids.intersection(target_ids)
            logging.info(f"Found {len(conflicting_ids)} conflicting ProjectInformation IDs")
            
            # Get source records
            source_db.cursor.execute('SELECT * FROM ProjectInformation')
            source_project_info = source_db.cursor.fetchall()
            
            source_db.cursor.execute('PRAGMA table_info(ProjectInformation)')
            column_names = [col[1] for col in source_db.cursor.fetchall()]
            
            # Process each source record
            for row in source_project_info:
                row_dict = {col: val for col, val in zip(column_names, row)}
                original_id = row_dict.get('ProjectInformation_id')
                
                if not original_id:
                    continue
                
                # Make a copy of row_dict with the original ID
                new_row_dict = row_dict.copy()
                
                # Modify ProjectName to indicate source
                if 'ProjectName' in new_row_dict and new_row_dict['ProjectName']:
                    source_db_name = os.path.basename(source_db._db_path).replace('.db', '')
                    new_row_dict['ProjectName'] = f"{new_row_dict['ProjectName']} (From {source_db_name})"
                
                # Handle based on whether there's a conflict
                if original_id in conflicting_ids:
                    # Create a new record with new ID for conflicted records
                    cols_without_id = [col for col in new_row_dict.keys() if col.lower() != 'projectinformation_id']
                    values_without_id = [new_row_dict[col] for col in cols_without_id]
                    
                    col_str = ', '.join(f'"{col}"' for col in cols_without_id)
                    val_str = ', '.join('?' for _ in cols_without_id)
                    
                    # Insert without specifying ID to get a new auto-generated ID
                    insert_sql = f'INSERT INTO ProjectInformation ({col_str}) VALUES ({val_str})'
                    target_db.cursor.execute(insert_sql, values_without_id)
                    
                    # Get the new ID
                    target_db.cursor.execute('SELECT last_insert_rowid()')
                    new_id = target_db.cursor.fetchone()[0]
                    
                    # Map old ID to new ID
                    id_mapping[original_id] = new_id
                    logging.info(f"ID conflict resolved: mapped source ID {original_id} to new ID {new_id}")
                else:
                    # No conflict - preserve the original ID by explicitly specifying it
                    cols = ', '.join(f'"{col}"' for col in new_row_dict.keys())
                    vals = ', '.join('?' for _ in new_row_dict.values())
                    
                    insert_sql = f'INSERT INTO ProjectInformation ({cols}) VALUES ({vals})'
                    target_db.cursor.execute(insert_sql, list(new_row_dict.values()))
                    
                    # Map ID to itself (identity mapping)
                    id_mapping[original_id] = original_id
                    logging.info(f"Preserved original ProjectInformation_id: {original_id}")
            
            target_db.commit()
            return id_mapping
        
        except Exception as e:
            target_db.rollback()
            logging.error(f"Error merging ProjectInformation: {e}")
            raise

    @staticmethod
    def ensure_project_information_table(db: DatabaseConnection) -> None:
        """Ensure ProjectInformation table exists with the correct structure."""
        try:
            # Check if table exists
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation'")
            if not db.cursor.fetchone():
                # Create table with required fields
                db.cursor.execute('''
                    CREATE TABLE "ProjectInformation" (
                        "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                        "ProjectName" TEXT,
                        "DisciplineModel" TEXT
                    )
                ''')
                logging.info("Created ProjectInformation table")
                
                # Add a default project record
                db_name = os.path.basename(db._db_path).replace('.db', '')
                db.cursor.execute('''
                    INSERT INTO "ProjectInformation" ("ProjectName", "DisciplineModel")
                    VALUES (?, 'Default')
                ''', (f"Project {db_name}",))
                logging.info(f"Added default project record for {db_name}")
            else:
                # Table exists, check for proper ID column
                db.cursor.execute("PRAGMA table_info('ProjectInformation')")
                columns = db.cursor.fetchall()
                column_names = [col[1].lower() for col in columns]
                
                # If ProjectInformation_id doesn't exist but id does, rename it
                if 'projectinformation_id' not in column_names and 'id' in column_names:
                    # Create a new table with the correct column name
                    column_defs = []
                    for col in columns:
                        col_name = col[1]
                        col_type = col[2]
                        
                        if col_name.lower() == 'id':
                            column_defs.append('"ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT')
                        else:
                            column_defs.append(f'"{col_name}" {col_type}')
                    
                    # Create temp table with correct column names
                    db.cursor.execute(f'''
                        CREATE TABLE "ProjectInformation_temp" (
                            {", ".join(column_defs)}
                        )
                    ''')
                    
                    # Copy data with renamed column
                    source_cols = []
                    target_cols = []
                    
                    for col in columns:
                        col_name = col[1]
                        if col_name.lower() == 'id':
                            source_cols.append('"id"')
                            target_cols.append('"ProjectInformation_id"')
                        else:
                            source_cols.append(f'"{col_name}"')
                            target_cols.append(f'"{col_name}"')
                    
                    db.cursor.execute(f'''
                        INSERT INTO "ProjectInformation_temp" ({", ".join(target_cols)})
                        SELECT {", ".join(source_cols)}
                        FROM "ProjectInformation"
                    ''')
                    
                    # Replace old table with new one
                    db.cursor.execute('DROP TABLE "ProjectInformation"')
                    db.cursor.execute('ALTER TABLE "ProjectInformation_temp" RENAME TO "ProjectInformation"')
                    logging.info("Renamed 'id' column to 'ProjectInformation_id' in ProjectInformation table")
                
                # Ensure the required columns exist
                if 'projectname' not in column_names:
                    db.cursor.execute('ALTER TABLE "ProjectInformation" ADD COLUMN "ProjectName" TEXT')
                    logging.info("Added ProjectName column to ProjectInformation table")
                
                if 'disciplinemodel' not in column_names:
                    db.cursor.execute('ALTER TABLE "ProjectInformation" ADD COLUMN "DisciplineModel" TEXT')
                    logging.info("Added DisciplineModel column to ProjectInformation table")
            
            # Ensure at least one row exists
            db.cursor.execute("SELECT COUNT(*) FROM ProjectInformation")
            if db.cursor.fetchone()[0] == 0:
                # Add a default project record if table is empty
                db_name = os.path.basename(db._db_path).replace('.db', '')
                db.cursor.execute('''
                    INSERT INTO "ProjectInformation" ("ProjectName", "DisciplineModel")
                    VALUES (?, 'Default')
                ''', (f"Project {db_name}",))
                logging.info(f"Added default project record for {db_name}")
                
            db.commit()
            logging.info("ProjectInformation table is ready")
        
        except sqlite3.Error as e:
            db.rollback()
            logging.error(f"Error ensuring ProjectInformation table: {e}")
            raise
    
    @staticmethod
    def update_sequences(db: DatabaseConnection) -> None:
        """Update SQLite sequences after inserting or copying records."""
        try:
            # Check if sqlite_sequence table exists
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'")
            if not db.cursor.fetchone():
                return  # No sequence table, nothing to update
            
            # Update ProjectInformation sequence
            try:
                db.cursor.execute("SELECT MAX(ProjectInformation_id) FROM ProjectInformation")
                max_id = db.cursor.fetchone()[0] or 0
                
                # Check if ProjectInformation exists in sqlite_sequence
                db.cursor.execute("SELECT COUNT(*) FROM sqlite_sequence WHERE name='ProjectInformation'")
                if db.cursor.fetchone()[0] > 0:
                    db.cursor.execute(
                        "UPDATE sqlite_sequence SET seq = ? WHERE name = 'ProjectInformation' AND seq < ?",
                        (max_id, max_id)
                    )
                else:
                    db.cursor.execute(
                        "INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
                        ("ProjectInformation", max_id)
                    )
                logging.info(f"Updated ProjectInformation sequence to {max_id}")
            except sqlite3.Error as e:
                logging.warning(f"Error updating ProjectInformation sequence: {e}")
            
            # Get list of tables with AUTOINCREMENT
            autoincrement_tables = []
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            for table in db.cursor.fetchall():
                table_name = table[0]
                if table_name in ['sqlite_sequence', 'ProjectInformation']:
                    continue
                    
                db.cursor.execute(f"PRAGMA table_info('{table_name}')")
                
                for col in db.cursor.fetchall():
                    if col[5] == 1:  # This is a primary key
                        autoincrement_tables.append(table_name)
                        break
            
            # Update sequences for each table
            for table_name in autoincrement_tables:
                try:
                    # Find the PRIMARY KEY column name
                    db.cursor.execute(f"PRAGMA table_info('{table_name}')")
                    pk_col = next((col[1] for col in db.cursor.fetchall() if col[5] == 1), None)
                    
                    if not pk_col:
                        continue
                    
                    # Get the maximum value for this primary key
                    db.cursor.execute(f'SELECT MAX("{pk_col}") FROM "{table_name}"')
                    max_value = db.cursor.fetchone()[0]
                    
                    if max_value is not None:
                        # Check if table exists in sqlite_sequence
                        db.cursor.execute(f"SELECT COUNT(*) FROM sqlite_sequence WHERE name='{table_name}'")
                        if db.cursor.fetchone()[0] > 0:
                            # Update sequence value if it's less than the max value
                            db.cursor.execute(f'''
                                UPDATE sqlite_sequence
                                SET seq = {max_value}
                                WHERE name = '{table_name}'
                                   AND seq < {max_value}
                            ''')
                        else:
                            # Insert new sequence entry
                            db.cursor.execute(
                                "INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
                                (table_name, max_value)
                            )
                        logging.info(f"Updated sequence for {table_name} to {max_value}")
                except sqlite3.Error as e:
                    logging.warning(f"Error updating sequence for {table_name}: {e}")
                    continue
            
            db.commit()
            logging.info("Successfully updated database sequences")
            
        except sqlite3.Error as e:
            db.rollback()
            logging.error(f"Error updating sequences: {e}")
            raise