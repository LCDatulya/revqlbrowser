import tkinter as tk
from tkinter import ttk, filedialog
from revql.application.utils.db_utils import get_table_data

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

        self.columns = ("Table Name", "Row Count")
        self.tree = ttk.Treeview(self.frame, columns=self.columns, show="headings")
        self.tree.heading("Table Name", text="Table Name")
        self.tree.heading("Row Count", text="Row Count", command=lambda: self.sort_by_column("Row Count", False))
        self.tree.grid(row=1, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=self.scrollbar.set)
        self.scrollbar.grid(row=1, column=4, sticky=(tk.N, tk.S))

    def run(self):
        self.root.mainloop()

    def display_table_data(self):
        db_path = self.db_path_entry.get()
        table_data = get_table_data(db_path)

        for row in self.tree.get_children():
            self.tree.delete(row)

        for table_name, count in table_data:
            self.tree.insert("", "end", values=(table_name, count))

    def browse_files(self):
        filename = filedialog.askopenfilename(
            initialdir="/",
            title="Select a Database",
            filetypes=(("SQLite Database Files", "*.db"), ("All Files", "*.*"))
        )
        self.db_path_entry.delete(0, tk.END)
        self.db_path_entry.insert(0, filename)

    def sort_by_column(self, col, descending):
        data = [(self.tree.set(child, col), child) for child in self.tree.get_children('')]
        data.sort(reverse=descending)
        for index, (val, child) in enumerate(data):
            self.tree.move(child, '', index)
        self.tree.heading(col, command=lambda: self.sort_by_column(col, not descending))