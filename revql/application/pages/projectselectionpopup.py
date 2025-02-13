import tkinter as tk
import sqlite3
from tkinter import ttk, messagebox
from ..utils.tablesorter import TableSorter
from ..relationmanagement.idrefactor import rename_id_columns_and_create_relations
from ..utils.db_utils import delete_empty_columns, delete_empty_tables
from ..utils.db_connection import DatabaseConnection
from ..relationmanagement.projectmanagement import ensure_project_information_id
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
        
            rename_id_columns_and_create_relations(self.db_path, self.data_matches)
            self.update_project_information_id()
        
        except Exception as e:
            print(f"Critical error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            raise

    def update_project_information_id(self):
        """Update ProjectInformation_id for new data."""
        db = DatabaseConnection(self.db_path)
        cursor = db.cursor
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            if table_name in ['ProjectInformation', 'sqlite_sequence']:
                continue
                
            try:
                cursor.execute(f'UPDATE "{table_name}" SET "ProjectInformation_id" = (SELECT "ProjectInformation_id" FROM "ProjectInformation" ORDER BY "ProjectInformation_id" DESC LIMIT 1) WHERE "ProjectInformation_id" IS NULL')
            except sqlite3.OperationalError as e:
                logging.warning(f"Could not update ProjectInformation_id in {table_name}: {e}")
        
        db.commit()
        db.close()
        messagebox.showinfo("Success", "ProjectInformation_id updated for new data.")