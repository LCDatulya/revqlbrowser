import tkinter as tk
from tkinter import ttk, messagebox
from revql.application.utils.db_connection import DatabaseConnection
from revql.application.utils.db_utils import delete_empty_columns
from revql.application.utils import AppStyles

class ColumnViewer:
    def __init__(self, parent, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name

        self.top = tk.Toplevel(parent)
        self.top.title(f"Columns in {table_name}")
        
        # Apply window defaults
        AppStyles.configure_window_defaults(self.top)

        self.frame = AppStyles.create_standard_frame(self.top)
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Treeview for columns
        tree_container, self.columns_tree = AppStyles.create_scrollable_treeview(
            self.frame,
            columns=["Column Name"],
            column_widths=[200]
        )
        tree_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Buttons
        button_panel, _, buttons = AppStyles.create_control_panel(
            self.frame,
            label_texts=[],
            button_texts=["Delete Selected Columns"],
            button_commands=[self.delete_selected_columns]
        )
        button_panel.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(10, 0))

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