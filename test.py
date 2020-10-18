# %%
import numpy as np
from partitionplan import PartitionPlan
import spar

action = [0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8, 0, 9, 0, 10, 0, 11, 0, 12, 0, 13, 0, 14, 0, 15, 0, 16, 0, 17,
          0, 18, 0, 19, 0, 20]

# %%
pp = PartitionPlan(10, 5000, 20)

pp.partition_ids()

# %%
pp = spar.add_node(pp, 0)
pp = spar.add_node(pp, 1)

# %%


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
