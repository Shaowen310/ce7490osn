# %%
import numpy as np
import snap
from graph import Graph
from partitionplan import PartitionPlan
import randalloc as spar

# %%
action = [
    0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8, 0, 9, 0, 10, 0, 11, 0, 12,
    0, 13, 0, 14, 0, 15, 0, 16, 0, 17, 0, 18, 0, 19, 0, 20
]

pp = PartitionPlan(4, 21, 4)

pp.partition_ids()
G1 = snap.TUNGraph.New()
G = Graph(G1)

for i in range(0,len(action),2):
    node1 = action[i]
    node2 = action[i + 1]
    if not pp.contains_user(node1):
        pp = spar.add_node(pp, node1)
        G.add_node(node1)
    if not pp.contains_user(node2):
        pp = spar.add_node(pp, node2)
        G.add_node(node2)
    G.add_edge(node1, node2)
    pp = spar.add_edge(pp, node1, node2, G)

print("done")