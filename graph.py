# wrapper of snap


class Graph:
    def __init__(self, g):
        self.g = g

    def get_neighbors(self, user_id):
        node = self.g.GetNI(user_id)
        return list(node.GetOutEdges())

    def add_node(self, user_id):
        if not self.g.IsNode(user_id):
            self.g.AddNode(user_id)

    def add_edge(self, user_id1, user_id2):
        self.g.AddEdge(user_id1, user_id2)

    # def save(self,fold):

