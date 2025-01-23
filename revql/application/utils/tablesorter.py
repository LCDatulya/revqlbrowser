class TableSorter:
    def __init__(self, tree):
        self.tree = tree

    def sort_by_column(self, col, descending, data_type):
        if data_type == 'numeric':
            data = [(float(self.tree.set(child, col)), child) for child in self.tree.get_children('')]
        else:
            data = [(self.tree.set(child, col), child) for child in self.tree.get_children('')]

        data.sort(reverse=descending)
        for index, (val, child) in enumerate(data):
            self.tree.move(child, '', index)
        self.tree.heading(col, command=lambda: self.sort_by_column(col, not descending, data_type))