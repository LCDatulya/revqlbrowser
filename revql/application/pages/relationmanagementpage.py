import tkinter as tk
from tkinter import ttk

class RelationManagementPage:
    def __init__(self, parent, matching_info):
        self.top = tk.Toplevel(parent)
        self.top.title("Manage Relationships")

        self.frame = ttk.Frame(self.top, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.top.columnconfigure(0, weight=1)
        self.top.rowconfigure(0, weight=1)
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(self.frame, columns=("Table", "Column", "Matches Table", "Match Ratio"), show="headings")
        self.tree.heading("Table", text="Table")
        self.tree.heading("Column", text="Column")
        self.tree.heading("Matches Table", text="Matches Table")
        self.tree.heading("Match Ratio", text="Match Ratio")
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=self.scrollbar.set)
        self.scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

        for table, column, match_table, ratio in matching_info:
            self.tree.insert("", "end", values=(table, column, match_table, f"{ratio:.2f}"))

        self.close_button = ttk.Button(self.frame, text="Close", command=self.top.destroy)
        self.close_button.grid(row=1, column=0, sticky=tk.E)