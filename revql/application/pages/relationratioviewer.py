import tkinter as tk
from tkinter import ttk, messagebox
from ..utils.tablesorter import TableSorter
from ..relationmanagement.idrefactor import rename_id_columns_and_create_relations
from ..utils.db_utils import delete_empty_columns, delete_empty_tables
from ..utils.db_connection import DatabaseConnection
from ..relationmanagement.projectmanagement import ensure_project_information_id

class RelationRatioViewer:
    def __init__(self, parent, matching_info, db_path):
        self.top = tk.Toplevel(parent)
        self.top.title("Manage Relationships")
        self.db_path = db_path
        self.matching_info = matching_info

        self.frame = ttk.Frame(self.top, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.top.columnconfigure(0, weight=1)
        self.top.rowconfigure(0, weight=1)
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        # Create treeview
        self.tree = ttk.Treeview(self.frame, columns=("Table", "Column", "Matches Table", "Match Ratio"), show="headings")
        self.tree.heading("Table", text="Table", command=lambda: self.sorter.sort_by_column("Table", False, 'alphabetical'))
        self.tree.heading("Column", text="Column", command=lambda: self.sorter.sort_by_column("Column", False, 'alphabetical'))
        self.tree.heading("Matches Table", text="Matches Table", command=lambda: self.sorter.sort_by_column("Matches Table", False, 'alphabetical'))
        self.tree.heading("Match Ratio", text="Match Ratio", command=lambda: self.sorter.sort_by_column("Match Ratio", False, 'numeric'))
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Add scrollbar
        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=self.scrollbar.set)
        self.scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Populate data
        for table, column, match_table, ratio in matching_info:
            self.tree.insert("", "end", values=(table, column, match_table, f"{ratio:.2f}"))

        # Initialize sorter
        self.sorter = TableSorter(self.tree)
        self.sorter.sort_by_column("Match Ratio", True, 'numeric')

        # Add buttons
        self.create_relations_button = ttk.Button(self.frame, text="Create Relations", command=self.create_relations)
        self.create_relations_button.grid(row=1, column=0, sticky=tk.W)

        self.close_button = ttk.Button(self.frame, text="Close", command=self.top.destroy)
        self.close_button.grid(row=1, column=1, sticky=tk.E)

    def create_relations(self):
        try:
            # Ask for confirmation
            if not messagebox.askyesno("Confirm Changes", 
                "This will delete empty tables and columns, then create relations. Continue?"):
                return
    
            db = DatabaseConnection(self.db_path)
            
            try:
                # Delete empty tables
                empty_tables = delete_empty_tables(self.db_path)
                if empty_tables:
                    messagebox.showinfo("Deleted Empty Tables", 
                        f"Deleted the following empty tables:\n\n{', '.join(empty_tables)}")
    
                # Delete empty columns
                cursor = db.cursor
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    
                    # Skip sqlite_sequence table
                    if table_name == 'sqlite_sequence':
                        continue
                    
                    delete_empty_columns(self.db_path, table_name)
    
                # Create relations
                rename_id_columns_and_create_relations(self.db_path, self.matching_info)
                
                # Ensure every table has ProjectInformation_id column
                ensure_project_information_id(self.db_path)
                
                messagebox.showinfo("Success", 
                    "Operations completed successfully:\n- Empty tables deleted\n- Empty columns deleted\n- Relations created\n- ProjectInformation_id added")
                
            finally:
                db.close()
                
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            raise