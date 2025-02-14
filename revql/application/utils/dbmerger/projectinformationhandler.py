from typing import Dict, Optional
from ..db_connection import DatabaseConnection
import sqlite3
import logging

class ProjectInformationHandler:
    @staticmethod
    def ensure_project_information_table(db: DatabaseConnection) -> None:
        """Handle ProjectInformation table setup"""
        try:
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation'")
            table_exists = db.cursor.fetchone() is not None

            if not table_exists:
                db.cursor.execute('''
                    CREATE TABLE "ProjectInformation" (
                        "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                        "OrganizationName" TEXT,
                        "ProjectIssueDate" TEXT,
                        "ProjectStatus" TEXT,
                        "ClientName" TEXT,
                        "ProjectAddress" TEXT,
                        "ProjectName" TEXT,
                        "ProjectNumber" TEXT,
                        "ClientAddress" TEXT,
                        "ChannelDefinitions" TEXT
                    )
                ''')
                logging.info("Created ProjectInformation table")

            db.commit()

        except Exception as e:
            db.rollback()
            logging.error(f"Error ensuring ProjectInformation table: {e}")
            raise

    @staticmethod
    def merge_project_information(source_db: DatabaseConnection, target_db: DatabaseConnection) -> Dict[int, int]:
        """Merge ProjectInformation records"""
        id_mapping = {}
        try:
            source_db.cursor.execute('SELECT * FROM ProjectInformation')
            source_project_info = source_db.cursor.fetchall()

            source_db.cursor.execute('PRAGMA table_info(ProjectInformation)')
            column_names = [col[1] for col in source_db.cursor.fetchall()]

            for row in source_project_info:
                row_dict = {col: val for col, val in zip(column_names, row)}
                original_id = row_dict['ProjectInformation_id']

                columns = ', '.join(f'"{col}"' for col in row_dict.keys())
                placeholders = ', '.join('?' for _ in row_dict)
                insert_sql = f'INSERT OR REPLACE INTO ProjectInformation ({columns}) VALUES ({placeholders})'

                try:
                    target_db.cursor.execute(insert_sql, list(row_dict.values()))
                    id_mapping[original_id] = original_id
                except sqlite3.IntegrityError as e:
                    logging.warning(f"Conflict inserting ProjectInformation record: {e}")
                    continue

            target_db.commit()
            return id_mapping

        except Exception as e:
            target_db.rollback()
            logging.error(f"Error merging ProjectInformation: {e}")
            raise

    @staticmethod
    def update_sequences(db: DatabaseConnection) -> None:
        """Update sequences after merging"""
        try:
            db.cursor.execute("DELETE FROM sqlite_sequence WHERE name='ProjectInformation'")
            db.cursor.execute("SELECT MAX(ProjectInformation_id) FROM ProjectInformation")
            max_id = db.cursor.fetchone()[0] or 0
            db.cursor.execute(
                "INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
                ("ProjectInformation", max_id)
            )
            db.commit()
        except sqlite3.Error as e:
            logging.error(f"Error updating sequences: {e}")
            raise