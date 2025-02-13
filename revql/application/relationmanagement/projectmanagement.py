from ..utils.db_connection import DatabaseConnection
from ..utils.db_utils import delete_empty_tables, delete_empty_columns
import logging
import sqlite3

def ensure_project_information_id(db_path):
    """
    Ensures that the ProjectInformation_id column exists in all tables and updates its values.
    """
    db = DatabaseConnection(db_path)
    cursor = db.cursor

    try:
        logging.debug(f"Ensuring ProjectInformation_id in database: {db_path}")

        # Delete empty tables
        logging.debug("Deleting empty tables")
        delete_empty_tables(db_path)

        # Delete empty columns
        logging.debug("Deleting empty columns")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            if table_name == 'sqlite_sequence':
                continue
            delete_empty_columns(db_path, table_name)

        # Check if ProjectInformation table exists
        logging.debug("Checking if ProjectInformation table exists")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation';")
        project_info_table = cursor.fetchone()
        if not project_info_table:
            raise Exception("ProjectInformation table does not exist in the database.")

        # Get the primary key column of ProjectInformation table
        logging.debug("Getting primary key column of ProjectInformation table")
        cursor.execute("PRAGMA table_info('ProjectInformation');")
        project_info_columns = cursor.fetchall()
        project_info_primary_key = None
        for column in project_info_columns:
            if column[1].lower() == 'projectinformation_id':
                project_info_primary_key = column[1]
                break

        if not project_info_primary_key:
            raise Exception("Primary key column 'ProjectInformation_id' not found in ProjectInformation table.")

        # Ensure ProjectInformation_id is the primary key
        logging.debug("Ensuring ProjectInformation_id is the primary key")
        cursor.execute(f"PRAGMA table_info('ProjectInformation');")
        columns = cursor.fetchall()
        if not any(col[1].lower() == 'projectinformation_id' and col[5] == 1 for col in columns):
            raise Exception("ProjectInformation_id is not set as the primary key in ProjectInformation table.")

        # Add ProjectInformation_id column to all tables
        logging.debug("Adding ProjectInformation_id column to all tables")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            if table_name in ['ProjectInformation', 'sqlite_sequence']:
                continue

            logging.debug(f"Processing table: {table_name}")
            cursor.execute(f'PRAGMA table_info("{table_name}");')
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'ProjectInformation_id' not in column_names:
                try:
                    logging.info(f"Adding ProjectInformation_id to table {table_name}")
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "ProjectInformation_id" INTEGER')
                except sqlite3.OperationalError as e:
                    logging.warning(f"Could not add ProjectInformation_id to {table_name}: {e}")
            else:
                logging.debug(f"ProjectInformation_id already exists in table {table_name}")

            # Update ProjectInformation_id column with the most recent ProjectInformation_id
            try:
                logging.info(f"Updating ProjectInformation_id in table {table_name}")
                cursor.execute(f'''
                    UPDATE "{table_name}"
                    SET "ProjectInformation_id" = (
                        SELECT "ProjectInformation_id"
                        FROM "ProjectInformation"
                        ORDER BY "ProjectInformation_id" DESC
                        LIMIT 1
                    )
                    WHERE "ProjectInformation_id" IS NULL
                ''')
            except sqlite3.OperationalError as e:
                logging.warning(f"Could not update ProjectInformation_id to {table_name}: {e}")

        db.commit()
        logging.info("Successfully ensured ProjectInformation_id in all tables.")

    except Exception as e:
        db.rollback()
        logging.error(f"Error ensuring ProjectInformation_id: {e}")
        raise

    finally:
        db.close()