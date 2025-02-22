import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from revql.application.utils.db_connection import DatabaseConnection
from revql.application.utils.db_utils import get_table_data, count_tables
from revql.application.utils.tablesorter import TableSorter
from revql.application.pages.relationratioviewer import RelationRatioViewer
from revql.application.pages.column_viewer import ColumnViewer
from ..utils.dbmerger import DatabaseMerger
from revql.application.relationmanagement.idrefactor import rename_id_columns_and_create_relations
import logging
from revql.application.utils.db_utils import find_matching_table_column_names

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
        item = self.tree.selection()[0]
        table_name = self.tree.item(item, "values")[0]
        self.show_columns_window(table_name)

    def show_columns_window(self, table_name):
        db_path = self.db_path_entry.get()
        if not db_path:
            messagebox.showwarning("No Database", "Please select a database first.")
            return

        ColumnViewer(self.root, db_path, table_name)

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
                              "This will create relationships in both databases and then merge them. Continue?"):
            try:
                # First, create relationships in source database
                matching_info_source = find_matching_table_column_names(source_db)
                if matching_info_source[1]:  # Check if there are data matches
                    rename_id_columns_and_create_relations(source_db, matching_info_source[1])
                else:
                    messagebox.showinfo("Info", "No relationships found in source database.")
                
                # Then, create relationships in target database
                matching_info_target = find_matching_table_column_names(self.db_path_entry.get())
                if matching_info_target[1]:  # Check if there are data matches
                    rename_id_columns_and_create_relations(self.db_path_entry.get(), matching_info_target[1])
                else:
                    messagebox.showinfo("Info", "No relationships found in target database.")
                
                # Finally, perform the merge
                merger = DatabaseMerger(source_db, self.db_path_entry.get())
                if merger.merge_databases():
                    messagebox.showinfo("Success", "Databases processed and merged successfully!")
                    self.display_table_data()  # Refresh the display
                else:
                    messagebox.showerror("Error", "Failed to merge databases. Check the logs for details.")
                    
            except Exception as e:
                messagebox.showerror("Error", f"An error occurred during the process: {str(e)}")
                logging.error(f"Database merge error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    app = TableViewerApp()
    app.run()