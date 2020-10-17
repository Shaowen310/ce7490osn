from node import Node

class Partition:
    def __init__(self, id):
        self.id = id
        self.nodes = []

    def __len__(self):
        return len(self.nodes)

    def num_masters(self):
        count = 0
        for n in self.nodes:
            if n.master:
                count+=1

        return count

    def add_master(self, user):
        self.nodes.append(Node(user, True))