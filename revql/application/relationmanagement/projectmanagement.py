from ..utils.db_connection import DatabaseConnection
from ..utils.db_utils import delete_empty_tables, delete_empty_columns

from ..utils.db_connection import DatabaseConnection
from ..utils.db_utils import delete_empty_tables, delete_empty_columns

from ..utils.db_connection import DatabaseConnection
from ..utils.db_utils import delete_empty_tables, delete_empty_columns

from ..utils.db_connection import DatabaseConnection
from ..utils.db_utils import delete_empty_tables, delete_empty_columns

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
        if column[1].lower() == 'projectinformation_id':
            project_info_primary_key = column[1]
            break

    if not project_info_primary_key:
        raise Exception("Primary key column 'ProjectInformation_id' not found in ProjectInformation table.")

    # Ensure ProjectInformation_id is the primary key
    cursor.execute(f"PRAGMA table_info('ProjectInformation');")
    columns = cursor.fetchall()
    if not any(col[1].lower() == 'projectinformation_id' and col[5] == 1 for col in columns):
        raise Exception("ProjectInformation_id is not set as the primary key in ProjectInformation table.")

    # Add ProjectInformation_id column to all tables
    for table in tables:
        table_name = table[0]
        if table_name in ['ProjectInformation', 'sqlite_sequence']:
            continue

        cursor.execute(f'PRAGMA table_info("{table_name}");')
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]

        if 'ProjectInformation_id' not in column_names:
            cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "ProjectInformation_id" INTEGER')

        # Update ProjectInformation_id column with the most recent ProjectInformation_id
        cursor.execute(f'UPDATE "{table_name}" SET "ProjectInformation_id" = (SELECT "{project_info_primary_key}" FROM "ProjectInformation" ORDER BY "{project_info_primary_key}" DESC LIMIT 1) WHERE "ProjectInformation_id" IS NULL')

        # Create new table with foreign key constraint
        new_columns = [f'"{col[1]}" {col[2]}' for col in columns]
        new_columns.append('"ProjectInformation_id" INTEGER')
        cursor.execute(f'''
            CREATE TABLE "{table_name}_new" (
                {", ".join(new_columns)},
                FOREIGN KEY("ProjectInformation_id") REFERENCES "ProjectInformation"("{project_info_primary_key}")
            )
        ''')

        # Insert data into new table
        old_columns = [f'"{col}"' for col in column_names]
        cursor.execute(f'''
            INSERT INTO "{table_name}_new" ({", ".join(old_columns)}, "ProjectInformation_id")
            SELECT {", ".join(old_columns)}, "ProjectInformation_id"
            FROM "{table_name}"
        ''')

        # Drop old table and rename new table
        cursor.execute(f'DROP TABLE "{table_name}"')
        cursor.execute(f'ALTER TABLE "{table_name}_new" RENAME TO "{table_name}"')

    db.commit()
    db.close()