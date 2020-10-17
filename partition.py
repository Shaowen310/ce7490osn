class Partition:
    def __init__(self):
        self.master_nodes = []
        self.slave_nodes = []

    def __len__(self):
        return len(self.master_nodes) + len(self.slave_nodes)

    def num_masters(self):
        return len(self.master_nodes)

    def add_master(self, user):
        self.master_nodes.append(user)

    def add_slave(self, user):
        self.slave_nodes.append(user)
