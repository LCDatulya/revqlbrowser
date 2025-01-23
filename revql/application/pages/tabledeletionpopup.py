from tkinter import messagebox

def confirm_delete_empty_tables(empty_tables):
    if empty_tables:
        confirm = messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete the following empty tables?\n\n{', '.join(empty_tables)}")
        return confirm
    else:
        messagebox.showinfo("No Empty Tables", "There are no empty tables to delete.")
        return False