import tkinter as tk
import sqlite3
from tkinter import ttk, messagebox
from ..utils.tablesorter import TableSorter
from ..relationmanagement.idrefactor import rename_id_columns_and_create_relations
from ..utils.db_utils import delete_empty_columns, delete_empty_tables
from ..utils.db_connection import DatabaseConnection
import logging

class RelationRatioViewer:
    def __init__(self, parent, matching_info, db_path):
        self.top = tk.Toplevel(parent)
        self.top.title("Manage Relationships")
        self.db_path = db_path
        self.name_matches, self.data_matches = matching_info  # Unpack the tuple of matches

        self.frame = ttk.Frame(self.top, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.top.columnconfigure(0, weight=1)
        self.top.rowconfigure(0, weight=1)
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        # Create treeview
        columns = ["Table", "Column", "Matches Table", "Match Ratio", "Data Overlap %"]
        self.tree = ttk.Treeview(self.frame, columns=columns, show="headings")
        
        # Configure column headings
        for col in columns:
            self.tree.heading(col, text=col, 
                            command=lambda c=col: self._sort_treeview(self.tree, c, 
                                data_type='numeric' if c in ["Match Ratio", "Data Overlap %"] else 'alphabetical'))
            if col in ["Match Ratio", "Data Overlap %"]:
                self.tree.column(col, width=100, anchor="center")
            else:
                self.tree.column(col, width=150)

        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Add scrollbar
        scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Populate matches - only show data matches with high overlap
        if self.data_matches:  # Check if data_matches exists and is not empty
            for table, column, match_table, ratio, overlap in self.data_matches:
                if overlap >= 95:  # Only show matches with 95% or higher overlap
                    self.tree.insert("", "end", 
                        values=(table, column, match_table, f"{ratio:.2f}", f"{overlap:.2f}"))

        # Sort tree by data overlap initially
        self._sort_treeview(self.tree, "Data Overlap %", "numeric")

        # Add buttons
        self.create_relations_button = ttk.Button(self.frame, text="Create Relations", command=self.create_relations)
        self.create_relations_button.grid(row=1, column=0, sticky=tk.W, pady=5)

        self.close_button = ttk.Button(self.frame, text="Close", command=self.top.destroy)
        self.close_button.grid(row=1, column=1, sticky=tk.E, pady=5)

    def _sort_treeview(self, tree, column, data_type='alphabetical'):
        sorter = TableSorter(tree)
        sorter.sort_by_column(column, False, data_type)

    def create_relations(self):
        """Create primary keys (if needed) and establish foreign key relationships between tables."""
        try:
            if not self.data_matches:
                messagebox.showinfo("No Matches", "No valid matches found to create relations.")
                return

            if not messagebox.askyesno("Confirm Changes",
                                       "This will modify tables and create relationships. Continue?"):
                return

            # Step 1: Rename ID columns and create relationships
            rename_id_columns_and_create_relations(self.db_path, self.data_matches)

            # Step 2: Ensure ProjectInformation_id exists and is updated in all tables
            self.ensure_project_information_id()

            # Show success message
            messagebox.showinfo("Success", "Relationships and ProjectInformation_id have been successfully updated.")

            # Auto-fetch table data in the parent window
            if hasattr(self.top.master, "display_table_data"):
                self.top.master.display_table_data()

            # Close the popup window
            self.top.destroy()

        except Exception as e:
            logging.error(f"Error creating relations: {e}")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            raise

    def ensure_project_information_id(self):
        """Ensure ProjectInformation_id exists in all tables and is updated for ALL rows."""
        db = DatabaseConnection(self.db_path)
        cursor = db.cursor
        
        try:
            # Step 1: Check if ProjectInformation table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation'")
            if not cursor.fetchone():
                # Create ProjectInformation table if it doesn't exist
                cursor.execute('''
                    CREATE TABLE "ProjectInformation" (
                        "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                        "ProjectName" TEXT,
                        "DisciplineModel" TEXT
                    )
                ''')
                logging.info("Created ProjectInformation table with ProjectInformation_id column")
            
            # Step 2: Check if ProjectInformation_id column exists in ProjectInformation table
            cursor.execute('PRAGMA table_info("ProjectInformation")')
            columns = cursor.fetchall()
            column_names = [col[1].lower() for col in columns]
            actual_column_names = [col[1] for col in columns]
            
            if 'projectinformation_id' not in column_names:
                # Check if there's an 'id' column that needs to be renamed
                if 'id' in column_names:
                    # First, create a list of all columns from the original table
                    column_defs = []
                    id_index = -1
                    
                    # Find the id column and prepare column definitions
                    for i, col in enumerate(columns):
                        col_name = col[1]
                        col_type = col[2]
                        
                        if col_name.lower() == 'id':
                            id_index = i
                            # This will become ProjectInformation_id
                            column_defs.append('"ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT')
                        else:
                            # Keep the column as is, preserving its type
                            column_defs.append(f'"{col_name}" {col_type}')
                            
                    # Ensure required columns exist
                    required_columns = {'projectname': False, 'disciplinemodel': False}
                    for col in columns:
                        col_lower = col[1].lower()
                        if col_lower in required_columns:
                            required_columns[col_lower] = True
                    
                    # Add missing required columns
                    for col, exists in required_columns.items():
                        if not exists:
                            column_defs.append(f'"{col.capitalize()}" TEXT')
                    
                    # Create the temp table with all columns
                    create_sql = f'''
                        CREATE TABLE "ProjectInformation_temp" (
                            {", ".join(column_defs)}
                        )
                    '''
                    cursor.execute(create_sql)
                    
                    # Prepare INSERT statement that maps id to ProjectInformation_id
                    source_cols = []
                    target_cols = []
                    
                    for i, col in enumerate(columns):
                        col_name = col[1]
                        if col_name.lower() == 'id':
                            target_cols.append('"ProjectInformation_id"')
                        else:
                            target_cols.append(f'"{col_name}"')
                        source_cols.append(f'"{col_name}"')
                    
                    # Handle any missing required columns in the INSERT
                    for col, exists in required_columns.items():
                        if not exists:
                            target_cols.append(f'"{col.capitalize()}"')
                            source_cols.append("NULL")  # Use NULL for missing columns
                    
                    # Execute the INSERT with proper column mapping
                    insert_sql = f'''
                        INSERT INTO "ProjectInformation_temp" ({", ".join(target_cols)})
                        SELECT {", ".join(source_cols)}
                        FROM "ProjectInformation"
                    '''
                    cursor.execute(insert_sql)
                    
                    # Replace original table
                    cursor.execute('DROP TABLE "ProjectInformation"')
                    cursor.execute('ALTER TABLE "ProjectInformation_temp" RENAME TO "ProjectInformation"')
                    logging.info("Renamed 'id' column to 'ProjectInformation_id' in ProjectInformation table")
                else:
                    # No id column exists - add ProjectInformation_id column
                    try:
                        # SQLite doesn't allow adding PRIMARY KEY via ALTER TABLE
                        # So we need to create a new table with all existing columns plus ProjectInformation_id
                        column_defs = ['"ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT']
                        
                        # Add all existing columns
                        for col in columns:
                            col_name = col[1]
                            col_type = col[2]
                            column_defs.append(f'"{col_name}" {col_type}')
                        
                        # Create temp table with all columns plus ProjectInformation_id
                        create_sql = f'''
                            CREATE TABLE "ProjectInformation_temp" (
                                {", ".join(column_defs)}
                            )
                        '''
                        cursor.execute(create_sql)
                        
                        # Prepare INSERT statement to copy data
                        col_names = [f'"{col[1]}"' for col in columns]
                        
                        # Execute INSERT
                        if col_names:
                            insert_sql = f'''
                                INSERT INTO "ProjectInformation_temp" ({", ".join(col_names)})
                                SELECT {", ".join(col_names)}
                                FROM "ProjectInformation"
                            '''
                            cursor.execute(insert_sql)
                        
                        # Replace original table
                        cursor.execute('DROP TABLE "ProjectInformation"')
                        cursor.execute('ALTER TABLE "ProjectInformation_temp" RENAME TO "ProjectInformation"')
                        logging.info("Added ProjectInformation_id column to ProjectInformation table")
                    except sqlite3.Error as e:
                        logging.error(f"Error adding ProjectInformation_id column: {e}")
                        raise
                    
            # Ensure required columns exist
            cursor.execute('PRAGMA table_info("ProjectInformation")')
            columns = cursor.fetchall()
            column_names = [col[1].lower() for col in columns]
            
            if 'projectname' not in column_names:
                cursor.execute('ALTER TABLE "ProjectInformation" ADD COLUMN "ProjectName" TEXT')
                logging.info("Added ProjectName column to ProjectInformation table")
            
            if 'disciplinemodel' not in column_names:
                cursor.execute('ALTER TABLE "ProjectInformation" ADD COLUMN "DisciplineModel" TEXT')
                logging.info("Added DisciplineModel column to ProjectInformation table")
            
            # Step 3: Make sure ProjectInformation has at least one row
            cursor.execute("SELECT COUNT(*) FROM ProjectInformation")
            count = cursor.fetchone()[0]
            
            if count == 0:
                # Create a default ProjectInformation record
                cursor.execute('''
                    INSERT INTO ProjectInformation 
                    (ProjectName, DisciplineModel) 
                    VALUES ('Default Project', 'Default')
                ''')
                cursor.execute("SELECT last_insert_rowid()")
                project_id = cursor.fetchone()[0]
                logging.info(f"Created default ProjectInformation record with ID {project_id}")
            else:
                # Get the ProjectInformation_id from the most recent record
                cursor.execute('''
                    SELECT "ProjectInformation_id" 
                    FROM "ProjectInformation" 
                    ORDER BY "ProjectInformation_id" DESC 
                    LIMIT 1
                ''')
                project_id = cursor.fetchone()[0]
            
            # Step 4: Ensure ProjectInformation_id exists in all other tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                if table_name in ['ProjectInformation', 'sqlite_sequence']:
                    continue
                    
                cursor.execute(f'PRAGMA table_info("{table_name}");')
                columns = cursor.fetchall()
                column_names = [col[1].lower() for col in columns]
        
                # Add ProjectInformation_id column if it doesn't exist
                if 'projectinformation_id' not in column_names:
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "ProjectInformation_id" INTEGER')
                    logging.info(f"Added ProjectInformation_id to table {table_name}")
        
                # Update ALL ProjectInformation_id values
                cursor.execute(f'''
                    UPDATE "{table_name}"
                    SET "ProjectInformation_id" = ?
                ''', (project_id,))
                    
                logging.info(f"Updated ALL ProjectInformation_id values in table {table_name}")
        
            db.commit()
            logging.info("Successfully ensured ProjectInformation_id in all tables and rows.")
        
        except sqlite3.Error as e:
            db.rollback()
            logging.error(f"Error ensuring ProjectInformation_id: {e}")
            raise
            
        finally:
            db.close()
            messagebox.showinfo("Success", "ProjectInformation_id updated for ALL rows in all tables.")