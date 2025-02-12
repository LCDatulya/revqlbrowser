import tkinter as tk
from tkinter import ttk, messagebox
from revql.application.utils.db_connection import DatabaseConnection

import tkinter as tk
from tkinter import ttk, messagebox
from revql.application.utils.db_connection import DatabaseConnection

class ProjectSelectionPopup:
    def __init__(self, parent, db_path, callback):
        self.db_path = db_path
        self.callback = callback

        self.top = tk.Toplevel(parent)
        self.top.title("Select Project")

        self.frame = ttk.Frame(self.top, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.top.columnconfigure(0, weight=1)
        self.top.rowconfigure(0, weight=1)
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        self.project_label = ttk.Label(self.frame, text="Select Project:")
        self.project_label.grid(row=0, column=0, sticky=tk.W)

        self.project_combobox = ttk.Combobox(self.frame)
        self.project_combobox.grid(row=0, column=1, sticky=(tk.W, tk.E))

        self.ok_button = ttk.Button(self.frame, text="OK", command=self.on_ok)
        self.ok_button.grid(row=1, column=0, columnspan=2, pady=5)

        self.load_projects()

    def load_projects(self):
        db = DatabaseConnection(self.db_path)
        cursor = db.cursor
        cursor.execute("SELECT ProjectInformation_id, ProjectNumber FROM ProjectInformation")
        projects = cursor.fetchall()
        self.project_combobox['values'] = [project[1] for project in projects]
        self.project_ids = {project[1]: project[0] for project in projects}

    def on_ok(self):
        selected_project_number = self.project_combobox.get()
        if not selected_project_number:
            messagebox.showwarning("No Selection", "Please select a project.")
            return

        project_id = self.project_ids[selected_project_number]
        self.callback(project_id)
        self.top.destroy()