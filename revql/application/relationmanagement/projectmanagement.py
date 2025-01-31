from ..utils.db_connection import DatabaseConnection
from ..utils.db_utils import delete_empty_tables, delete_empty_columns

def ensure_project_information_id(db_path):
    db = DatabaseConnection(db_path)
    cursor = db.cursor

    # Delete empty tables
    delete_empty_tables(db_path)

    # Delete empty columns
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        if table_name == 'sqlite_sequence':
            continue
        delete_empty_columns(db_path, table_name)

    # Check if ProjectInformation table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation';")
    project_info_table = cursor.fetchone()
    if not project_info_table:
        raise Exception("ProjectInformation table does not exist in the database.")

    # Get the primary key column of ProjectInformation table
    cursor.execute("PRAGMA table_info('ProjectInformation');")
    project_info_columns = cursor.fetchall()
    project_info_primary_key = None
    for column in project_info_columns:
        if column[5] == 1:  # Primary key column
            project_info_primary_key = column[1]
            break

    if not project_info_primary_key:
        raise Exception("ProjectInformation table does not have a primary key.")

    # Ensure every table has the ProjectInformation_id column
    for table in tables:
        table_name = table[0]
        if table_name in ['ProjectInformation', 'sqlite_sequence']:
            continue

        cursor.execute(f"PRAGMA table_info('{table_name}');")
        columns = cursor.fetchall()
        column_names = [column[1] for column in columns]

        if "ProjectInformation_id" not in column_names:
            cursor.execute(f"ALTER TABLE '{table_name}' ADD COLUMN ProjectInformation_id INTEGER")

        # Ensure ProjectInformation_id is a foreign key referencing ProjectInformation primary key
        cursor.execute(f"PRAGMA foreign_key_list('{table_name}');")
        foreign_keys = cursor.fetchall()
        foreign_key_exists = any(fk[3] == "ProjectInformation_id" and fk[2] == "ProjectInformation" for fk in foreign_keys)

        if not foreign_key_exists:
            cursor.execute(f"""
                ALTER TABLE '{table_name}'
                ADD CONSTRAINT fk_ProjectInformation
                FOREIGN KEY (ProjectInformation_id)
                REFERENCES ProjectInformation({project_info_primary_key})
            """)

    db.commit()
    db.close()

# Example usage
if __name__ == "__main__":
    db_path = "path_to_your_database.db"
    ensure_project_information_id(db_path)