import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
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
        self.show_table_data_window(table_name)

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

class TableDataViewer:
    def __init__(self, parent, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name

        self.top = tk.Toplevel(parent)
        self.top.title(f"Data in {table_name}")

        self.frame = ttk.Frame(self.top, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.top.columnconfigure(0, weight=1)
        self.top.rowconfigure(0, weight=1)
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        self.search_label = ttk.Label(self.frame, text="Search:")
        self.search_label.grid(row=0, column=0, sticky=tk.W)

        self.search_entry = ttk.Entry(self.frame, width=50)
        self.search_entry.grid(row=0, column=1, sticky=(tk.W, tk.E))

        self.search_button = ttk.Button(self.frame, text="Search", command=self.search_table)
        self.search_button.grid(row=0, column=2, sticky=tk.W)

        self.data_tree = ttk.Treeview(self.frame, show="headings", selectmode="extended")
        self.data_tree.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.data_tree.yview)
        self.data_tree.configure(yscroll=self.scrollbar.set)
        self.scrollbar.grid(row=1, column=3, sticky=(tk.N, tk.S))

        self.delete_row_button = ttk.Button(self.frame, text="Delete Selected Rows", command=self.delete_selected_rows)
        self.delete_row_button.grid(row=2, column=0, sticky=tk.W, pady=5)

        self.delete_column_button = ttk.Button(self.frame, text="Delete Selected Columns", command=self.delete_selected_columns)
        self.delete_column_button.grid(row=2, column=1, sticky=tk.W, pady=5)

        self.data_tree.bind("<Double-1>", self.on_cell_double_click)
        self.data_tree.bind("<Button-1>", self.on_column_click, add="+")

        self.load_table_data()

    def load_table_data(self):
        db = DatabaseConnection(self.db_path)
        cursor = db.cursor
        cursor.execute(f'SELECT * FROM "{self.table_name}"')
        rows = cursor.fetchall()

        cursor.execute(f'PRAGMA table_info("{self.table_name}")')
        columns = [col[1] for col in cursor.fetchall()]

        self.data_tree["columns"] = columns
        for col in columns:
            self.data_tree.heading(col, text=col)
            self.data_tree.column(col, width=100)

        for row in rows:
            self.data_tree.insert("", "end", values=row)

    def delete_selected_rows(self):
        selected_items = self.data_tree.selection()
        if not selected_items:
            messagebox.showwarning("No Selection", "Please select rows to delete.")
            return

        db = DatabaseConnection(self.db_path)
        cursor = db.cursor

        for item in selected_items:
            row_values = self.data_tree.item(item, "values")
            primary_key_value = row_values[0]  # Assuming the first column is the primary key
            cursor.execute(f'DELETE FROM "{self.table_name}" WHERE rowid = ?', (primary_key_value,))
            self.data_tree.delete(item)

        db.commit()
        db.close()
        messagebox.showinfo("Success", "Selected rows deleted successfully.")

    def delete_selected_columns(self):
        selected_columns = self.data_tree.selection()
        if not selected_columns:
            messagebox.showwarning("No Selection", "Please select columns to delete.")
            return

        db = DatabaseConnection(self.db_path)
        cursor = db.cursor

        for col in selected_columns:
            col_name = self.data_tree.heading(col, "text")
            cursor.execute(f'ALTER TABLE "{self.table_name}" DROP COLUMN "{col_name}"')
            self.data_tree.delete(col)

        db.commit()
        db.close()
        messagebox.showinfo("Success", "Selected columns deleted successfully.")

    def on_cell_double_click(self, event):
        item = self.data_tree.selection()[0]
        column = self.data_tree.identify_column(event.x)
        column_index = int(column.replace("#", "")) - 1
        column_name = self.data_tree["columns"][column_index]
        old_value = self.data_tree.item(item, "values")[column_index]

        new_value = simpledialog.askstring("Edit Cell", f"Enter new value for {column_name}:", initialvalue=old_value)
        if new_value is not None:
            self.update_cell_value(item, column_name, new_value)

    def update_cell_value(self, item, column_name, new_value):
        row_values = self.data_tree.item(item, "values")
        primary_key_value = row_values[0]  # Assuming the first column is the primary key

        db = DatabaseConnection(self.db_path)
        cursor = db.cursor
        cursor.execute(f'UPDATE "{self.table_name}" SET "{column_name}" = ? WHERE rowid = ?', (new_value, primary_key_value))
        db.commit()
        db.close()

        row_values = list(row_values)
        row_values[self.data_tree["columns"].index(column_name)] = new_value
        self.data_tree.item(item, values=row_values)

    def search_table(self):
        search_value = self.search_entry.get()
        if not search_value:
            messagebox.showwarning("No Search Value", "Please enter a value to search.")
            return

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
        for _, row in matches:
            self.data_tree.insert("", "end", values=row)

    def on_column_click(self, event):
        region = self.data_tree.identify_region(event.x, event.y)
        if region == "heading":
            column = self.data_tree.identify_column(event.x)
            column_index = int(column.replace("#", "")) - 1
            column_name = self.data_tree["columns"][column_index]

            # Select all items in the column
            for item in self.data_tree.get_children():
                self.data_tree.selection_add(item)
                self.data_tree.see(item)

            # Highlight the column header
            self.data_tree.heading(column, text=column_name, anchor="center")

if __name__ == "__main__":
    app = TableViewerApp()
    app.run()