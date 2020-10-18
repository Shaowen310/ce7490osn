# %%
import numpy as np
import snap
from graph import Graph
from partitionplan import PartitionPlan
import spar

action = [
    0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8, 0, 9, 0, 10, 0, 11, 0, 12,
    0, 13, 0, 14, 0, 15, 0, 16, 0, 17, 0, 18, 0, 19, 0, 20
]

# %%
pp = PartitionPlan(4, 5, 4)

pp.partition_ids()

# %%
pp = spar.add_node(pp, 0)
pp = spar.add_node(pp, 1)

# %%

G1 = snap.TNGraph.New()
G1.AddNode(0)
G1.AddNode(1)
G1.AddEdge(0, 1)

n0 = G1.GetNI(0)
out_nodes = list(n0.GetOutEdges())

g = Graph(G1)

# %%
spar.add_edge(pp, 0, 1, g)

# %%
# for i in range(len(action)):
#     node1 = action[i]
#     node2 = action[i + 1]
#     if not pp.contains_user(node1):
#         spar.add_node(pp, node1)
#     if not pp.contains_user(node2):
#         spar.add_node(pp, node2)
#     spar.add_edge(pp, node1, node2)
# %%
