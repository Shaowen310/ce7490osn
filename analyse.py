# %%
import numpy as np
from partitionplan import PartitionPlan

# %%

# %%
FOLDER = 'facebook_4'


palloc = np.load(FOLDER + '/palloc.npy')
ualloc = np.load(FOLDER + '/ualloc.npy')
u2p = np.load(FOLDER + '/u2p.npy')

pp = PartitionPlan()
pp.load(FOLDER)

n_replicas = pp.num_replicas()

print('num_replicas: ', n_replicas)

# %%
n_masters_per_server = pp.num_masters_per_partition()

print('num_masters_per_server:', n_masters_per_server)
# %%
