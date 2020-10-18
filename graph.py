# wrapper of snap
class Graph:
    def __init__(self, g):
        self.g = g

    def get_neighbors(self, user_id):
        node = self.g.GetNI(user_id)
        return list(node.GetOutEdges())
