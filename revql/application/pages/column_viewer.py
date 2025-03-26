import tkinter as tk
from tkinter import ttk, messagebox
from revql.application.utils.db_connection import DatabaseConnection
from revql.application.utils.db_utils import delete_empty_columns

class ColumnViewer:
    def __init__(self, parent, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name

        self.top = tk.Toplevel(parent)
        self.top.title(f"Columns in {table_name}")

        self.frame = ttk.Frame(self.top, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.top.columnconfigure(0, weight=1)
        self.top.rowconfigure(0, weight=1)
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        self.columns_tree = ttk.Treeview(self.frame, columns=("Column Name",), show="headings", selectmode="extended")
        self.columns_tree.heading("Column Name", text="Column Name")
        self.columns_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.columns_tree.yview)
        self.columns_tree.configure(yscroll=self.scrollbar.set)
        self.scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

        self.delete_button = ttk.Button(self.frame, text="Delete Selected Columns", command=self.delete_selected_columns)
        self.delete_button.grid(row=1, column=0, sticky=tk.W)

        self.load_columns()

    def load_columns(self):
        db = DatabaseConnection(self.db_path)
        cursor = db.cursor
        cursor.execute(f"PRAGMA table_info(\"{self.table_name}\");")
        columns = cursor.fetchall()

        for column in columns:
            self.columns_tree.insert("", "end", values=(column[1],))

    def delete_selected_columns(self):
        selected_items = self.columns_tree.selection()
        if not selected_items:
            messagebox.showwarning("No Selection", "Please select columns to delete.")
            return

        column_names = [self.columns_tree.item(item, "values")[0] for item in selected_items]
        db_path = self.db_path

        if messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete the columns '{', '.join(column_names)}' from table '{self.table_name}'?"):
            for item, column_name in zip(selected_items, column_names):
                delete_empty_columns(db_path, self.table_name, column_name)
                self.columns_tree.delete(item)
            messagebox.showinfo("Success", f"Columns '{', '.join(column_names)}' deleted successfully.")