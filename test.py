# %%
import numpy as np
from partitionplan import PartitionPlan
import spar

# %%
pp = PartitionPlan(10, 10, 20)

pp.partition_ids()
# %%
spar.add_node(pp, 1)
# %%
