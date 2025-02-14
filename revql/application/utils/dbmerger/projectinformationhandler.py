from typing import Dict
from ..db_connection import DatabaseConnection
import sqlite3
import logging

class ProjectInformationHandler:
    @staticmethod
    def ensure_project_information_table(db: DatabaseConnection) -> None:
        """Handle ProjectInformation table setup"""
        try:
            print("Ensuring ProjectInformation table exists")
            
            # Base columns
            base_columns = {
                "ProjectInformation_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
                "OrganizationName": "TEXT",
                "ProjectIssueDate": "TEXT", 
                "ProjectStatus": "TEXT",
                "ClientName": "TEXT",
                "ProjectAddress": "TEXT",
                "ProjectName": "TEXT",
                "ProjectNumber": "TEXT",
                "ClientAddress": "TEXT",
                "ChannelDefinitions": "TEXT"
            }
            
            # Additional columns
            additional_columns = {
                "DisciplineBIMCoordinator": "TEXT",
                "ElectricalServicesEngineer": "TEXT",
                "ElectricalServicesModeller": "TEXT",
                "FireServicesEngineer": "TEXT",
                "FireServicesModeller": "TEXT",
                "HydraulicServicesEngineer": "TEXT",
                "HydraulicServicesModeller": "TEXT",
                "MechanicalServicesEngineer": "TEXT",
                "MechanicalServicesModeller": "TEXT",
                "DesignProjectTeamLeader": "TEXT",
                "DisciplineModel": "TEXT"
            }
            
            # Check if table exists
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation'")
            if not db.cursor.fetchone():
                # Create table
                columns_sql = [f'"{name}" {type_}' for name, type_ in base_columns.items()]
                create_sql = f'''
                    CREATE TABLE "ProjectInformation" (
                        {", ".join(columns_sql)}
                    )
                '''
                db.cursor.execute(create_sql)
                
            # Add any missing columns
            db.cursor.execute('PRAGMA table_info(ProjectInformation)')
            existing_columns = {col[1]: col[2] for col in db.cursor.fetchall()}
            
            all_columns = {**base_columns, **additional_columns}
            for col_name, col_type in all_columns.items():
                if col_name not in existing_columns:
                    try:
                        alter_sql = f'ALTER TABLE "ProjectInformation" ADD COLUMN "{col_name}" {col_type}'
                        db.cursor.execute(alter_sql)
                        logging.info(f"Added column {col_name} to ProjectInformation")
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not add column {col_name}: {e}")
                        
            db.commit()
            print("ProjectInformation table setup complete")
            
        except Exception as e:
            db.rollback()
            logging.error(f"Error in ProjectInformation setup: {e}")
            raise
        
    def update_sequences(self, db: DatabaseConnection) -> None:
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