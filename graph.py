# wrapper of snap
import snap


class Graph:
    def __init__(self, g):
        self.g = g

    def get_neighbors(self, user_id):
        node = self.g.GetNI(user_id)
        return list(node.GetOutEdges())

    def add_node(self, user_id):
        self.g.AddNode(user_id)
        for NI in self.g.Nodes():
            print('k',NI.GetId())

    def add_edge(self, user_id1, user_id2):
        # node1 = self.g.GetNI(user_id1)
        # node2 = self.g.GetNI(user_id2)
        self.g.AddEdge(user_id1, user_id2)
