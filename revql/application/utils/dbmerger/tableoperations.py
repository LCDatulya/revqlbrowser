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
        """Copy table logic with single-line SQL to avoid syntax issues"""
        temp_name = f"{table_name}_temp_{int(time.time())}"

        try:
            logging.info(f"Copying table {table_name}")

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
                    # Remove AUTOINCREMENT if it creates issues and use PRIMARY KEY only.
                    col_defs.append(f'"{col_name}" {col_type} PRIMARY KEY')
                else:
                    col_defs.append(f'"{col_name}" {col_type}"')

            # Ensure ProjectInformation_id exists
            if 'projectinformation_id' not in processed_cols:
                col_defs.append('"ProjectInformation_id" INTEGER')
                col_names.append('"ProjectInformation_id"')

            # Create temporary table
            create_sql = f'CREATE TABLE "{temp_name}" ({", ".join(col_defs)}, FOREIGN KEY("ProjectInformation_id") REFERENCES "ProjectInformation"("ProjectInformation_id"))'
            target_db.cursor.execute(create_sql)

            # Build SELECT query from source table
            select_cols = []
            for col_name in col_names:
                if col_name.lower() != '"projectinformation_id"':
                    select_cols.append(col_name)
            case_stmt = []
            for old_id, new_id in id_mapping.items():
                case_stmt.append(f"WHEN {old_id} THEN {new_id}")
            select_sql = ", ".join(select_cols)
            if case_stmt:
                select_sql += f', CASE "ProjectInformation_id" {" ".join(case_stmt)} ELSE "ProjectInformation_id" END'
            else:
                select_sql += ', "ProjectInformation_id"'

            # Build the INSERT query in one line
            insert_sql = "INSERT INTO \"{temp}\" ({cols}) SELECT {sel} FROM \"{tbl}\"".format(
                temp=temp_name,
                cols=", ".join(col_names),
                sel=select_sql,
                tbl=table_name
            )
            source_db.cursor.execute(insert_sql)

            # Replace original table with the temporary one
            target_db.cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            target_db.cursor.execute(f'ALTER TABLE "{temp_name}" RENAME TO "{table_name}"')
            target_db.commit()

        except Exception as e:
            logging.error(f"Error copying table {table_name}: {e}")
            raise

    @staticmethod
    def merge_existing_table(source_db: DatabaseConnection, target_db: DatabaseConnection, 
                             table_name: str, columns: List[tuple], id_mapping: Dict[int, int]) -> None:
        """Merge table logic handling duplicate columns and adding missing columns"""
        try:
            logging.info(f"Merging existing table {table_name}")

            # Special-case: if merging ProjectInformation, ensure DisciplineModel exists.
            if table_name.lower() == "projectinformation":
                target_db.cursor.execute('PRAGMA table_info("ProjectInformation")')
                cols = [col[1].lower() for col in target_db.cursor.fetchall()]
                if "disciplinemodel" not in cols:
                    try:
                        alter_sql = 'ALTER TABLE "ProjectInformation" ADD COLUMN "DisciplineModel" TEXT'
                        logging.info(f"Adding missing column DisciplineModel to {table_name}: {alter_sql}")
                        target_db.cursor.execute(alter_sql)
                        target_db.commit()
                        logging.info("Successfully added DisciplineModel column.")
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not add column DisciplineModel to {table_name}: {e}")

            # Get source columns (using lower-case keys for consistency)
            source_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
            source_columns = {}
            for col in source_db.cursor.fetchall():
                col_name = col[1]
                col_lower = col_name.lower()
                if col_lower not in source_columns:
                    source_columns[col_lower] = {
                        'name': col_name,
                        'type': col[2],
                        'notnull': col[3],
                        'pk': col[5]
                    }

            # Get target columns (using lower-case keys)
            target_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
            target_columns = {}
            for col in target_db.cursor.fetchall():
                col_name = col[1]
                col_lower = col_name.lower()
                if col_lower not in target_columns:
                    target_columns[col_lower] = {
                        'name': col_name,
                        'type': col[2],
                        'notnull': col[3],
                        'pk': col[5]
                    }

            # For every column in the source that is missing in the target, add it
            for col_lower, info in source_columns.items():
                if col_lower not in target_columns:
                    try:
                        col_def = f'"{info["name"]}" {info["type"]}'
                        if info["notnull"]:
                            col_def += ' NOT NULL DEFAULT ""'
                        alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN {col_def}'
                        logging.info(f"Adding missing column to {table_name}: {alter_sql}")
                        target_db.cursor.execute(alter_sql)
                        target_db.commit()
                        target_columns[col_lower] = info
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not add column {info['name']}: {e}")

            # Refresh source column positions for data mapping
            source_db.cursor.execute(f'PRAGMA table_info("{table_name}")')
            source_column_positions = {}
            seen_columns = set()
            for i, col in enumerate(source_db.cursor.fetchall()):
                col_name = col[1]
                col_lower = col_name.lower()
                if col_lower not in seen_columns:
                    source_column_positions[col_lower] = i
                    seen_columns.add(col_lower)

            # Build list of available columns (common to both source and target)
            available_columns = [col_lower for col_lower in source_columns.keys() if col_lower in target_columns]
            if not available_columns:
                logging.error(f"No matching columns found for table {table_name}")
                return

            # Build the INSERT statement using target column names
            columns_sql = ', '.join(f'"{target_columns[col_lower]["name"]}"' for col_lower in available_columns)
            placeholders = ', '.join('?' for _ in available_columns)
            insert_sql = f'INSERT OR IGNORE INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'

            # Process source data in batches and map data using the available columns
            source_db.cursor.execute(f'SELECT * FROM "{table_name}"')
            batch_size = 100
            while True:
                rows = source_db.cursor.fetchmany(batch_size)
                if not rows:
                    break
                batch_data = []
                for row in rows:
                    values = []
                    for col_lower in available_columns:
                        pos = source_column_positions.get(col_lower)
                        if pos is not None:
                            val = row[pos]
                            if col_lower == 'projectinformation_id' and val in id_mapping:
                                val = id_mapping[val]
                            values.append(val)
                        else:
                            values.append(None)
                    batch_data.append(values)
                try:
                    target_db.cursor.executemany(insert_sql, batch_data)
                    target_db.commit()
                except sqlite3.Error as e:
                    logging.error(f"Error inserting batch in {table_name}: {e}")
                    target_db.rollback()

            logging.info(f"Successfully merged table {table_name}")

        except Exception as e:
            target_db.rollback()
            logging.error(f"Error merging table {table_name}: {e}", exc_info=True)
            raise
        
    @staticmethod
    def table_exists(db: DatabaseConnection, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            result = db.cursor.fetchone()
            return result is not None
        except Exception as e:
            logging.error(f"Error checking if table exists: {e}")
            return False