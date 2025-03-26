import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from revql.application.utils.db_connection import DatabaseConnection
from revql.application.utils.db_utils import get_table_data, count_tables, find_matching_table_column_names, get_table_data, count_tables
from revql.application.utils.tablesorter import TableSorter
from revql.application.pages.relationratioviewer import RelationRatioViewer
from ..utils.dbmerger import DatabaseMerger
from revql.application.relationmanagement.idrefactor import rename_id_columns_and_create_relations
import logging
from revql.application.utils.db_utils import find_matching_table_column_names
import os
import sqlite3

class TableViewerApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("SQLite Table Data Viewer")

        self.frame = ttk.Frame(self.root, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.frame.columnconfigure(1, weight=1)
        self.frame.rowconfigure(1, weight=1)

        self.db_path_label = ttk.Label(self.frame, text="Database Path:")
        self.db_path_label.grid(row=0, column=0, sticky=tk.W)

        self.db_path_entry = ttk.Entry(self.frame, width=50)
        self.db_path_entry.grid(row=0, column=1, sticky=(tk.W, tk.E))

        self.browse_button = ttk.Button(self.frame, text="Browse", command=self.browse_files)
        self.browse_button.grid(row=0, column=2, sticky=tk.W)

        self.fetch_button = ttk.Button(self.frame, text="Fetch Table Data", command=self.display_table_data)
        self.fetch_button.grid(row=0, column=3, sticky=tk.W)

        self.create_relationships_button = ttk.Button(self.frame, text="Create Relations", command=self.create_relationships)
        self.create_relationships_button.grid(row=0, column=4, sticky=tk.W)
        
        self.merge_button = ttk.Button(self.frame, text="Merge Database", command=self.merge_database)
        self.merge_button.grid(row=0, column=5, sticky=tk.W)

        self.columns = ("Table Name", "Row Count", "Column Count")
        self.tree = ttk.Treeview(self.frame, columns=self.columns, show="headings")
        self.tree.heading("Table Name", text="Table Name", command=lambda: self.sorter.sort_by_column("Table Name", False, 'alphabetical'))
        self.tree.heading("Row Count", text="Row Count", command=lambda: self.sorter.sort_by_column("Row Count", False, 'numeric'))
        self.tree.heading("Column Count", text="Column Count", command=lambda: self.sorter.sort_by_column("Column Count", False, 'numeric'))
        self.tree.grid(row=1, column=0, columnspan=5, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=self.scrollbar.set)
        self.scrollbar.grid(row=1, column=5, sticky=(tk.N, tk.S))

        self.sorter = TableSorter(self.tree)

        self.table_count_label = ttk.Label(self.frame, text="Number of Tables: 0")
        self.table_count_label.grid(row=2, column=0, columnspan=5, sticky=tk.W)

        self.tree.bind("<Double-1>", self.on_table_double_click)

    def run(self):
        self.root.mainloop()

    def display_table_data(self):
        db_path = self.db_path_entry.get()
        db = DatabaseConnection(db_path)
        table_data = get_table_data(db_path)
        table_count = count_tables(db_path)

        for row in self.tree.get_children():
            self.tree.delete(row)

        for table_name, row_count, col_count in table_data:
            self.tree.insert("", "end", values=(table_name, row_count, col_count))

        self.table_count_label.config(text=f"Number of Tables: {table_count}")

    def create_relationships(self):
        db_path = self.db_path_entry.get()
        if not db_path:
            messagebox.showwarning("No Database", "Please select a database first.")
            return
        
        matching_info = find_matching_table_column_names(db_path)
        if matching_info:
            RelationRatioViewer(self.root, matching_info, db_path)
        else:
            messagebox.showinfo("No Matches", "No matching table-column names found.")

    def browse_files(self):
        filename = filedialog.askopenfilename(
            initialdir="/",
            title="Select a Database",
            filetypes=(("SQLite Database Files", "*.db"), ("All Files", "*.*"))
        )
        self.db_path_entry.delete(0, tk.END)
        self.db_path_entry.insert(0, filename)

    def on_table_double_click(self, event):
        """Handle double-click on a table row."""
        try:
            # Get the selected item
            selected_items = self.tree.selection()
            if not selected_items:
                # No item is selected, ignore the event
                return

            item = selected_items[0]
            table_name = self.tree.item(item, "values")[0]

            # Open the table data viewer
            self.show_table_data_window(table_name)
        except Exception as e:
            logging.error(f"Error handling double-click: {e}")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

    def show_table_data_window(self, table_name):
        db_path = self.db_path_entry.get()
        if not db_path:
            messagebox.showwarning("No Database", "Please select a database first.")
            return

        TableDataViewer(self.root, db_path, table_name)

    def merge_database(self):
        if not self.db_path_entry.get():
            messagebox.showwarning("No Target Database", "Please select a target database first.")
            return
    
        source_db = filedialog.askopenfilename(
            title="Select Source Database to Merge",
            filetypes=(("SQLite Database Files", "*.db"), ("All Files", "*.*"))
        )
        
        if not source_db:
            return
    
        if messagebox.askyesno("Confirm Merge", 
                              "This will merge the source database into the target database while preserving project information. Continue?"):
            try:
                # Perform the merge - the DatabaseMerger now handles all preparation steps
                merger = DatabaseMerger(source_db, self.db_path_entry.get())
                if merger.merge_databases():
                    messagebox.showinfo("Success", "Database processed and merged successfully!")
                    
                    # Auto-fetch table data
                    self.display_table_data()
                else:
                    messagebox.showerror("Error", "Failed to merge databases. Check the logs for details.")
                    
            except Exception as e:
                messagebox.showerror("Error", f"An error occurred during the process: {str(e)}")
                logging.error(f"Database merge error: {str(e)}", exc_info=True)
                
    def prepare_source_database(self, db_path):
        """Prepare source database by ensuring it has a valid ProjectInformation table."""
        db = DatabaseConnection(db_path)
        cursor = db.cursor
        
        try:
            # Check if ProjectInformation table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProjectInformation'")
            if not cursor.fetchone():
                # Create ProjectInformation table with proper primary key
                cursor.execute('''
                    CREATE TABLE "ProjectInformation" (
                        "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                        "ProjectName" TEXT,
                        "DisciplineModel" TEXT
                    )
                ''')

                # Add default row
                db_name = os.path.basename(db_path).replace('.db', '')
                cursor.execute('''
                    INSERT INTO "ProjectInformation" ("ProjectName", "DisciplineModel")
                    VALUES (?, 'Default')
                ''', (f"Source: {db_name}",))

                db.commit()
                logging.info(f"Created ProjectInformation table in source database: {db_path}")
            else:
                # Check if ProjectInformation_id exists and is a primary key
                cursor.execute("PRAGMA table_info('ProjectInformation')")
                columns = cursor.fetchall()
                has_pi_id = False
                has_id = False

                for col in columns:
                    col_name = col[1].lower()
                    is_pk = col[5] == 1

                    if col_name == 'projectinformation_id' and is_pk:
                        has_pi_id = True
                    elif col_name == 'id' and is_pk:
                        has_id = True

                # If there's no proper primary key, rebuild the table
                if not has_pi_id:
                    if has_id:
                        # Rename id to ProjectInformation_id
                        cursor.execute('''
                            CREATE TABLE "ProjectInformation_temp" (
                                "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                                "ProjectName" TEXT,
                                "DisciplineModel" TEXT
                            )
                        ''')

                        # Copy data, mapping id to ProjectInformation_id
                        cursor.execute('''
                            INSERT INTO "ProjectInformation_temp" 
                            ("ProjectInformation_id", "ProjectName", "DisciplineModel")
                            SELECT "id", 
                                   COALESCE("ProjectName", 'Unknown Project'),
                                   COALESCE("DisciplineModel", 'Default')
                            FROM "ProjectInformation"
                        ''')

                        # Replace original table
                        cursor.execute('DROP TABLE "ProjectInformation"')
                        cursor.execute('ALTER TABLE "ProjectInformation_temp" RENAME TO "ProjectInformation"')
                    else:
                        # Create new table with proper structure
                        cursor.execute('''
                            CREATE TABLE "ProjectInformation_temp" (
                                "ProjectInformation_id" INTEGER PRIMARY KEY AUTOINCREMENT,
                                "ProjectName" TEXT,
                                "DisciplineModel" TEXT
                            )
                        ''')

                        # Try to preserve any existing data
                        try:
                            cursor.execute('''
                                INSERT INTO "ProjectInformation_temp" 
                                ("ProjectName", "DisciplineModel")
                                SELECT 
                                    COALESCE("ProjectName", 'Unknown Project'),
                                    COALESCE("DisciplineModel", 'Default')
                                FROM "ProjectInformation"
                            ''')
                        except sqlite3.OperationalError:
                            # If columns don't exist, add a default row
                            db_name = os.path.basename(db_path).replace('.db', '')
                            cursor.execute('''
                                INSERT INTO "ProjectInformation_temp" ("ProjectName", "DisciplineModel")
                                VALUES (?, 'Default')
                            ''', (f"Source: {db_name}",))

                        # Replace original table
                        cursor.execute('DROP TABLE "ProjectInformation"')
                        cursor.execute('ALTER TABLE "ProjectInformation_temp" RENAME TO "ProjectInformation"')

                    db.commit()
                    logging.info(f"Rebuilt ProjectInformation table in source database: {db_path}")

            # Ensure at least one row exists
            cursor.execute("SELECT COUNT(*) FROM ProjectInformation")
            count = cursor.fetchone()[0]

            if count == 0:
                # Add a default row
                db_name = os.path.basename(db_path).replace('.db', '')
                cursor.execute('''
                    INSERT INTO "ProjectInformation" ("ProjectName", "DisciplineModel")
                    VALUES (?, 'Default')
                ''', (f"Source: {db_name}",))
                db.commit()
                logging.info(f"Added default ProjectInformation record to source database: {db_path}")
        
        except Exception as e:
            db.rollback()
            logging.error(f"Error preparing source database: {e}")
            raise
        finally:
            db.close()

class TableDataViewer:
    def __init__(self, parent, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name
        self.selected_cell = None  # Track selected cell (item_id, column)

        self.top = tk.Toplevel(parent)
        self.top.title(f"Data in {table_name}")
        self.top.geometry("900x600")  # Set a reasonable default size

        # Configure a more appealing color scheme
        bg_color = "#f5f5f5"
        self.top.configure(bg=bg_color)

        self.frame = ttk.Frame(self.top, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=10)

        self.top.columnconfigure(0, weight=1)
        self.top.rowconfigure(0, weight=1)
        self.frame.columnconfigure(1, weight=1)
        self.frame.rowconfigure(1, weight=1)

        # Search controls with improved styling
        search_frame = ttk.Frame(self.frame)
        search_frame.grid(row=0, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 10))
        search_frame.columnconfigure(1, weight=1)

        self.search_label = ttk.Label(search_frame, text="Search:", font=("Segoe UI", 10))
        self.search_label.grid(row=0, column=0, sticky=tk.W, padx=(0, 5))

        self.search_entry = ttk.Entry(search_frame, width=50, font=("Segoe UI", 10))
        self.search_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        self.search_entry.bind("<Return>", lambda e: self.search_table())

        self.search_button = ttk.Button(search_frame, text="Search", command=self.search_table)
        self.search_button.grid(row=0, column=2, sticky=tk.W, padx=5)

        self.clear_button = ttk.Button(search_frame, text="Clear", command=self.clear_search)
        self.clear_button.grid(row=0, column=3, sticky=tk.W, padx=5)

        # Configure Treeview with grid styling
        self.data_tree = ttk.Treeview(self.frame, show="headings", selectmode="browse")
        self.data_tree.grid(row=1, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Add vertical scrollbar
        y_scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.data_tree.yview)
        self.data_tree.configure(yscroll=y_scrollbar.set)
        y_scrollbar.grid(row=1, column=4, sticky=(tk.N, tk.S))

        # Add horizontal scrollbar
        x_scrollbar = ttk.Scrollbar(self.frame, orient=tk.HORIZONTAL, command=self.data_tree.xview)
        self.data_tree.configure(xscroll=x_scrollbar.set)
        x_scrollbar.grid(row=2, column=0, columnspan=4, sticky=(tk.W, tk.E))

        # Button frame with improved layout
        button_frame = ttk.Frame(self.frame)
        button_frame.grid(row=3, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=10)

        self.delete_row_button = ttk.Button(button_frame, text="Delete Selected Row", command=self.delete_selected_rows)
        self.delete_row_button.pack(side=tk.LEFT, padx=5)

        self.delete_column_button = ttk.Button(button_frame, text="Delete Selected Column", command=self.delete_selected_columns)
        self.delete_column_button.pack(side=tk.LEFT, padx=5)

        self.refresh_button = ttk.Button(button_frame, text="Refresh Data", command=self.refresh_data)
        self.refresh_button.pack(side=tk.LEFT, padx=5)

        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        status_bar = ttk.Label(self.frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=4, column=0, columnspan=5, sticky=(tk.W, tk.E), pady=(5, 0))

        # Set up event bindings for cell selection
        self.data_tree.bind("<ButtonRelease-1>", self.on_cell_click)
        self.data_tree.bind("<Double-1>", self.on_cell_double_click)
        self.data_tree.bind("<Return>", self.on_enter_key)
        self.data_tree.bind("<KeyPress>", self.on_key_press)

        # Apply custom styles
        self.apply_treeview_style()
        
        # Load data
        self.load_table_data()

    def apply_treeview_style(self):
        """Apply custom style to Treeview for grid-like appearance with better cell selection."""
        style = ttk.Style()
        
        # Configure the main Treeview style
        style.configure("Treeview", 
                         background="#ffffff",
                         foreground="#000000",
                         rowheight=25,
                         fieldbackground="#ffffff",
                         borderwidth=1,
                         font=('Segoe UI', 10))
        
        # Configure the Treeview heading style
        style.configure("Treeview.Heading",
                         background="#e6e6e6",
                         foreground="#000000",
                         relief="flat",
                         font=('Segoe UI', 10, 'bold'))
        
        # Map states to different visual appearances
        style.map("Treeview",
                  background=[("selected", "#e1f5fe")],
                  foreground=[("selected", "#000000")])

    def load_table_data(self):
        self.status_var.set(f"Loading data from {self.table_name}...")
        
        db = DatabaseConnection(self.db_path)
        cursor = db.cursor
        try:
            cursor.execute(f'SELECT * FROM "{self.table_name}"')
            rows = cursor.fetchall()

            cursor.execute(f'PRAGMA table_info("{self.table_name}")')
            columns = [col[1] for col in cursor.fetchall()]

            # Configure columns in the treeview
            self.data_tree["columns"] = columns
            for col in columns:
                self.data_tree.heading(col, text=col, anchor="center")
                self.data_tree.column(col, width=100, anchor="center", stretch=True)

            # Insert data rows
            for row in rows:
                self.data_tree.insert("", "end", values=row, tags=("row",))

            # Apply alternating row colors for better readability
            for i, item in enumerate(self.data_tree.get_children()):
                if i % 2 == 0:
                    self.data_tree.item(item, tags=("evenrow",))
                else:
                    self.data_tree.item(item, tags=("oddrow",))

            # Configure row tags
            self.data_tree.tag_configure("evenrow", background="#f0f0f0")
            self.data_tree.tag_configure("oddrow", background="#ffffff")
            self.data_tree.tag_configure("selected_cell", background="#bbdefb", foreground="#000000")
            
            self.status_var.set(f"Loaded {len(rows)} rows from {self.table_name}")
        except Exception as e:
            self.status_var.set(f"Error loading data: {str(e)}")
            messagebox.showerror("Error", f"Failed to load table data: {str(e)}")
        finally:
            db.close()

    def highlight_cell(self, item, column):
        """Highlight the selected cell and clear previous selection."""
        # Reset all row tags
        for i, row_id in enumerate(self.data_tree.get_children()):
            if i % 2 == 0:
                self.data_tree.item(row_id, tags=("evenrow",))
            else:
                self.data_tree.item(row_id, tags=("oddrow",))
        
        # Select the row containing the cell
        self.data_tree.selection_set(item)
        
        # Store the selected cell information
        self.selected_cell = (item, column)
        
        # Update status bar with selected cell info
        col_name = self.data_tree["columns"][int(column.replace("#", "")) - 1]
        row_values = self.data_tree.item(item, "values")
        cell_value = row_values[int(column.replace("#", "")) - 1]
        self.status_var.set(f"Selected: {col_name} = {cell_value}")
        
        # Create visual highlight effect for the cell
        self.data_tree.after(50, lambda: self.flash_selected_cell(item))

    def flash_selected_cell(self, item):
        """Create a visual flash effect to highlight the selected cell."""
        current_tags = self.data_tree.item(item, "tags")
        if "selected_cell_flash" in current_tags:
            # Remove the flash highlight
            new_tags = [tag for tag in current_tags if tag != "selected_cell_flash"]
            self.data_tree.item(item, tags=new_tags)
        else:
            # Add the flash highlight
            self.data_tree.item(item, tags=(*current_tags, "selected_cell_flash"))
            self.data_tree.tag_configure("selected_cell_flash", background="#bbdefb")

    def on_cell_click(self, event):
        """Handle cell click events to select individual cells."""
        region = self.data_tree.identify_region(event.x, event.y)
        if region == "cell":
            item = self.data_tree.identify_row(event.y)
            column = self.data_tree.identify_column(event.x)
            self.highlight_cell(item, column)

    def on_cell_double_click(self, event):
        """Handle double-click events to edit cells."""
        region = self.data_tree.identify_region(event.x, event.y)
        if region == "cell":
            item = self.data_tree.identify_row(event.y)
            column = self.data_tree.identify_column(event.x)
            column_index = int(column.replace("#", "")) - 1
            column_name = self.data_tree["columns"][column_index]
            old_value = self.data_tree.item(item, "values")[column_index]

            # Store current selection before opening dialog
            self.highlight_cell(item, column)
            
            # Edit the cell value
            new_value = simpledialog.askstring("Edit Cell", 
                                               f"Edit value for {column_name}:", 
                                               initialvalue=old_value,
                                               parent=self.top)
            if new_value is not None:
                self.update_cell_value(item, column_name, new_value)

    def on_enter_key(self, event):
        """Handle Enter key to edit the selected cell."""
        if self.selected_cell:
            item, column = self.selected_cell
            column_index = int(column.replace("#", "")) - 1
            column_name = self.data_tree["columns"][column_index]
            old_value = self.data_tree.item(item, "values")[column_index]
            
            new_value = simpledialog.askstring("Edit Cell", 
                                               f"Edit value for {column_name}:", 
                                               initialvalue=old_value,
                                               parent=self.top)
            if new_value is not None:
                self.update_cell_value(item, column_name, new_value)

    def on_key_press(self, event):
        """Handle arrow keys to navigate between cells."""
        if not self.selected_cell:
            return
            
        item, column = self.selected_cell
        column_index = int(column.replace("#", "")) - 1
        items = self.data_tree.get_children()
        item_index = items.index(item)
        
        # Handle arrow key navigation
        if event.keysym == 'Up' and item_index > 0:
            # Move to cell above
            new_item = items[item_index - 1]
            self.highlight_cell(new_item, column)
            self.data_tree.see(new_item)
            
        elif event.keysym == 'Down' and item_index < len(items) - 1:
            # Move to cell below
            new_item = items[item_index + 1]
            self.highlight_cell(new_item, column)
            self.data_tree.see(new_item)
            
        elif event.keysym == 'Left' and column_index > 0:
            # Move to cell to the left
            new_column = f"#{column_index}"
            self.highlight_cell(item, new_column)
            
        elif event.keysym == 'Right' and column_index < len(self.data_tree["columns"]) - 1:
            # Move to cell to the right
            new_column = f"#{column_index + 2}"
            self.highlight_cell(item, new_column)

    def update_cell_value(self, item, column_name, new_value):
        """Update a cell value in the database and UI."""
        row_values = self.data_tree.item(item, "values")
        primary_key_value = row_values[0]  # Assuming the first column is the primary key

        try:
            db = DatabaseConnection(self.db_path)
            cursor = db.cursor
            cursor.execute(f'UPDATE "{self.table_name}" SET "{column_name}" = ? WHERE rowid = ?', (new_value, primary_key_value))
            db.commit()
            db.close()

            # Update the treeview
            row_values = list(row_values)
            row_values[self.data_tree["columns"].index(column_name)] = new_value
            self.data_tree.item(item, values=row_values)
            
            self.status_var.set(f"Updated {column_name} to '{new_value}'")
            
            # Flash the cell to provide visual feedback
            self.flash_selected_cell(item)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update cell: {str(e)}")
            self.status_var.set(f"Error updating cell: {str(e)}")

    def delete_selected_rows(self):
        """Delete selected rows from the database and UI."""
        selected_items = self.data_tree.selection()
        if not selected_items:
            messagebox.showwarning("No Selection", "Please select a row to delete.")
            return

        if not messagebox.askyesno("Confirm Delete", "Are you sure you want to delete the selected row(s)?"):
            return

        try:
            db = DatabaseConnection(self.db_path)
            cursor = db.cursor

            for item in selected_items:
                row_values = self.data_tree.item(item, "values")
                primary_key_value = row_values[0]  # Assuming the first column is the primary key
                cursor.execute(f'DELETE FROM "{self.table_name}" WHERE rowid = ?', (primary_key_value,))
                self.data_tree.delete(item)

            db.commit()
            db.close()
            self.status_var.set(f"Deleted {len(selected_items)} row(s)")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete row(s): {str(e)}")
            self.status_var.set(f"Error deleting row(s): {str(e)}")

    def delete_selected_columns(self):
        """Delete the selected column from the database and UI."""
        if not self.selected_cell:
            messagebox.showwarning("No Selection", "Please select a cell in the column to delete.")
            return
            
        item, column = self.selected_cell
        column_index = int(column.replace("#", "")) - 1
        column_name = self.data_tree["columns"][column_index]
        
        if column_index == 0:
            messagebox.showwarning("Cannot Delete", "Cannot delete the primary key column.")
            return
            
        if not messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete the column '{column_name}'?"):
            return

        try:
            db = DatabaseConnection(self.db_path)
            cursor = db.cursor
            cursor.execute(f'ALTER TABLE "{self.table_name}" DROP COLUMN "{column_name}"')
            db.commit()
            db.close()
            
            # Refresh the view
            self.refresh_data()
            self.status_var.set(f"Deleted column '{column_name}'")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete column: {str(e)}")
            self.status_var.set(f"Error deleting column: {str(e)}")

    def search_table(self):
        """Search the table for matching data."""
        search_value = self.search_entry.get()
        if not search_value:
            messagebox.showwarning("No Search Value", "Please enter a value to search.")
            return

        self.status_var.set(f"Searching for '{search_value}'...")
        
        try:
            db = DatabaseConnection(self.db_path)
            cursor = db.cursor
            cursor.execute(f'SELECT * FROM "{self.table_name}"')
            rows = cursor.fetchall()

            cursor.execute(f'PRAGMA table_info("{self.table_name}")')
            columns = [col[1] for col in cursor.fetchall()]

            matches = []
            for row in rows:
                match_score = sum(search_value.lower() in str(value).lower() for value in row)
                if match_score > 0:
                    matches.append((match_score, row))

            matches.sort(reverse=True, key=lambda x: x[0])

            self.data_tree.delete(*self.data_tree.get_children())
            for i, (_, row) in enumerate(matches):
                item_id = self.data_tree.insert("", "end", values=row)
                if i % 2 == 0:
                    self.data_tree.item(item_id, tags=("evenrow",))
                else:
                    self.data_tree.item(item_id, tags=("oddrow",))

            self.status_var.set(f"Found {len(matches)} matching rows")
            self.selected_cell = None  # Clear cell selection
            
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
            self.status_var.set(f"Search error: {str(e)}")

    def clear_search(self):
        """Clear search and show all data."""
        self.search_entry.delete(0, tk.END)
        self.refresh_data()

    def refresh_data(self):
        """Refresh the table data."""
        self.data_tree.delete(*self.data_tree.get_children())
        self.selected_cell = None
        self.load_table_data()
        self.status_var.set("Data refreshed")

if __name__ == "__main__":
    app = TableViewerApp()
    app.run()