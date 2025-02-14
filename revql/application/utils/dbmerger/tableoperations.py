from typing import Dict, List, Tuple
from ..db_connection import DatabaseConnection
import sqlite3
import logging
import time

class TableOperations:
    MAX_RETRIES = 3
    RETRY_DELAY = 0.5
    
    @staticmethod
    def copy_table(source_db: DatabaseConnection, target_db: DatabaseConnection, 
                   table_name: str, columns: List[tuple], id_mapping: Dict[int, int]) -> None:
        """Copy table logic"""
        temp_name = f"{table_name}_temp_{int(time.time())}"
        
        try:
            print(f"Copying table {table_name}")
            
            # Generate column definitions
            col_defs = []
            col_names = []
            processed_cols = set()
            
            for col in columns:
                col_name = col[1]
                col_lower = col_name.lower()
                
                if col_lower in processed_cols:
                    continue
                    
                processed_cols.add(col_lower)
                col_type = col[2]
                col_names.append(f'"{col_name}"')
                
                if col[5]:  # Is primary key
                    col_defs.append(f'"{col_name}" {col_type} PRIMARY KEY AUTOINCREMENT')
                else:
                    col_defs.append(f'"{col_name}" {col_type}')

            # Add ProjectInformation_id if needed
            if 'projectinformation_id' not in processed_cols:
                col_defs.append('"ProjectInformation_id" INTEGER')
                col_names.append('"ProjectInformation_id"')

            # Create table
            create_sql = f'CREATE TABLE "{temp_name}" ({", ".join(col_defs)})'
            target_db.cursor.execute(create_sql)

            # Copy data with ID mapping
            select_cols = []
            for col_name in col_names:
                if col_name.lower() != '"projectinformation_id"':
                    select_cols.append(col_name)
            
            case_stmt = []
            for old_id, new_id in id_mapping.items():
                case_stmt.append(f"WHEN {old_id} THEN {new_id}")
                
            select_sql = ", ".join(select_cols)
            if case_stmt:
                select_sql += f''', CASE "ProjectInformation_id" {" ".join(case_stmt)} 
                                  ELSE "ProjectInformation_id" END'''
            else:
                select_sql += ', "ProjectInformation_id"'

            insert_sql = f'''
                INSERT INTO "{temp_name}" ({", ".join(col_names)})
                SELECT {select_sql}
                FROM "{table_name}"
            '''
            source_db.cursor.execute(insert_sql)

            # Replace original table
            target_db.cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            target_db.cursor.execute(f'ALTER TABLE "{temp_name}" RENAME TO "{table_name}"')

        except Exception as e:
            logging.error(f"Error copying table {table_name}: {e}")
            raise

    @staticmethod
    def merge_existing_table(source_db: DatabaseConnection, target_db: DatabaseConnection,
                            table_name: str, columns: List[tuple], id_mapping: Dict[int, int]) -> None:
        """Merge table logic"""
        try:
            print(f"Merging existing table {table_name}")
        
            # Get data from source table
            source_db.cursor.execute(f'SELECT * FROM "{table_name}"')
            source_data = source_db.cursor.fetchall()
            
            # Get column names
            source_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
            source_columns = [col[1] for col in source_db.cursor.fetchall()]
            
            for row in source_data:
                row_dict = dict(zip(source_columns, row))
                if 'ProjectInformation_id' in row_dict:
                    old_id = row_dict['ProjectInformation_id']
                    row_dict['ProjectInformation_id'] = id_mapping.get(old_id, old_id)
                    
                columns = ', '.join(f'"{col}"' for col in row_dict.keys())
                placeholders = ', '.join('?' for _ in row_dict)
                insert_sql = f'INSERT OR IGNORE INTO "{table_name}" ({columns}) VALUES ({placeholders})'
                
                target_db.cursor.execute(insert_sql, list(row_dict.values()))
                
            target_db.commit()
            print(f"Successfully merged table {table_name}")
            
        except Exception as e:
            target_db.rollback()
            logging.error(f"Error merging table {table_name}: {e}")
            raise