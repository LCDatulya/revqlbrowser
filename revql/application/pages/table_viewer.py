import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import sqlite3
from revql.application.utils.db_utils import get_table_data
from revql.application.utils.tablecounter import count_tables
from revql.application.utils.tabledeleter import delete_empty_tables, delete_single_column_or_row_tables
from revql.application.utils.tablesorter import TableSorter
from revql.application.pages.tabledeletionpopup import confirm_delete_empty_tables
from revql.application.relationmanagement.matchratiocalc import find_matching_table_column_names
from revql.application.pages.relationmanagementpage import RelationManagementPage

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

        self.delete_empty_button = ttk.Button(self.frame, text="Delete Empty Tables", command=self.confirm_delete_empty_tables)
        self.delete_empty_button.grid(row=0, column=4, sticky=tk.W)

        self.delete_single_column_or_row_button = ttk.Button(self.frame, text="Delete Single Column/Row Tables", command=self.confirm_delete_single_column_or_row_tables)
        self.delete_single_column_or_row_button.grid(row=0, column=5, sticky=tk.W)

        self.create_relationships_button = ttk.Button(self.frame, text="Create Relationships", command=self.create_relationships)
        self.create_relationships_button.grid(row=0, column=6, sticky=tk.W)

        self.columns = ("Table Name", "Row Count", "Column Count")
        self.tree = ttk.Treeview(self.frame, columns=self.columns, show="headings")
        self.tree.heading("Table Name", text="Table Name", command=lambda: self.sorter.sort_by_column("Table Name", False, 'alphabetical'))
        self.tree.heading("Row Count", text="Row Count", command=lambda: self.sorter.sort_by_column("Row Count", False, 'numeric'))
        self.tree.heading("Column Count", text="Column Count", command=lambda: self.sorter.sort_by_column("Column Count", False, 'numeric'))
        self.tree.grid(row=1, column=0, columnspan=7, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=self.scrollbar.set)
        self.scrollbar.grid(row=1, column=7, sticky=(tk.N, tk.S))

        self.sorter = TableSorter(self.tree)

        self.table_count_label = ttk.Label(self.frame, text="Number of Tables: 0")
        self.table_count_label.grid(row=2, column=0, columnspan=7, sticky=tk.W)

    def run(self):
        self.root.mainloop()

    def display_table_data(self):
        db_path = self.db_path_entry.get()
        table_data = get_table_data(db_path)
        table_count = count_tables(db_path)

        for row in self.tree.get_children():
            self.tree.delete(row)

        for table_name, row_count, col_count in table_data:
            self.tree.insert("", "end", values=(table_name, row_count, col_count))

        self.table_count_label.config(text=f"Number of Tables: {table_count}")

    def confirm_delete_empty_tables(self):
        db_path = self.db_path_entry.get()
        empty_tables = self.get_empty_tables(db_path)
        if confirm_delete_empty_tables(empty_tables):
            self.delete_empty_tables()

    def confirm_delete_single_column_or_row_tables(self):
        db_path = self.db_path_entry.get()
        single_column_or_row_tables = self.get_single_column_or_row_tables(db_path)
        if confirm_delete_empty_tables(single_column_or_row_tables):
            self.delete_single_column_or_row_tables()

    def get_empty_tables(self, db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get the list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        empty_tables = []

        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\";")
            count = cursor.fetchone()[0]
            if count == 0:
                empty_tables.append(table_name)

        conn.close()
        return empty_tables

    def get_single_column_or_row_tables(self, db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get the list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        single_column_or_row_tables = []

        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\";")
            row_count = cursor.fetchone()[0]
            cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
            col_count = len(cursor.fetchall())
            if row_count == 1 or col_count == 1:
                single_column_or_row_tables.append(table_name)

        conn.close()
        return single_column_or_row_tables

    def delete_empty_tables(self):
        db_path = self.db_path_entry.get()
        empty_tables = delete_empty_tables(db_path)
        if empty_tables:
            messagebox.showinfo("Deleted Tables", f"Deleted empty tables: {', '.join(empty_tables)}")
        self.display_table_data()

    def delete_single_column_or_row_tables(self):
        db_path = self.db_path_entry.get()
        single_column_or_row_tables = delete_single_column_or_row_tables(db_path)
        if single_column_or_row_tables:
            messagebox.showinfo("Deleted Tables", f"Deleted single column/row tables: {', '.join(single_column_or_row_tables)}")
        self.display_table_data()

    def create_relationships(self):
        db_path = self.db_path_entry.get()
        if not db_path:
            messagebox.showwarning("No Database", "Please select a database first.")
            return
        
        matching_info = find_matching_table_column_names(db_path)
        if matching_info:
            RelationManagementPage(self.root, matching_info)
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