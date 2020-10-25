# %%
import numpy as np
from partitionplan import PartitionPlan
import re
import matplotlib.pyplot as plt


# %%
plt.style.use('seaborn-whitegrid')

rx_replica_create = r'(replica create )(\d+)'


# %%
FOLDER = 'facebook_4'
TEXT = 'facebook4.txt'

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
replica_create = []

with open(TEXT) as f:
    line = f.readline()
    while line:
        replica_create.append(int(re.search(rx_replica_create, line).groups()[1]))
        line = f.readline()

fig = plt.figure()
ax = plt.axes()
ax.set_xlabel('edge creation events')
ax.set_ylabel('number of replicas created')

x = list(range(len(replica_create)))

ax.plot(x, replica_create)

# %%
