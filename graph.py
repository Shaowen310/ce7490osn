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

    def remove_node(self, user_id):
        try:
            for i in self.get_neighbors(user_id):
                self.remove_edge(user_id, i)
        except Exception:
            pass
        if self.g.IsNode(user_id):
            self.g.DelNode(user_id)

    def remove_edge(self, user_id1, user_id2):
        self.g.DelEdge(user_id1, user_id2)

    # def save(self,fold):
