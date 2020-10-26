# %%
import numpy as np
from partitionplan import PartitionPlan
import re
import matplotlib.pyplot as plt


# %%
rx_replica_create = r'(replica create )(\d+)'

# %%
FOLDER = 'lastfm_new_4'
TEXT = 'facebook_new512.txt'


palloc = np.load(FOLDER + '/palloc.npy')
ualloc = np.load(FOLDER + '/ualloc.npy')
u2p = np.load(FOLDER + '/u2p.npy')

pp = PartitionPlan()
pp.load(FOLDER)

n_replicas = pp.num_replicas()

print('num_replicas: ', n_replicas)
print('avg_num_slave_replicas_per_user: ', n_replicas/np.count_nonzero(ualloc)-1)

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

replica_create = np.array(replica_create)

# %%
plt.style.use('seaborn-whitegrid')
fig = plt.figure()
ax = plt.axes()
ax.set_xlabel('edge creation events')
ax.set_xlim((0,len(replica_create)))
ax.set_ylabel('number of replicas created')
ax.set_ylim((0,np.max(replica_create)))

x = list(range(len(replica_create)))

ax.plot(x, replica_create)

# %%
hist, bin_edges = np.histogram(replica_create, bins=1000)

cumsum = np.cumsum(hist)
cumsum = np.hstack(([0], cumsum))
cumsum = cumsum / cumsum[-1]

print('prob equal or fewer than 2 replica creations: ', np.count_nonzero(replica_create<=2)/len(replica_create))

# %%
fig = plt.figure()
ax = plt.axes()

ax.set_xlabel('number of replicas created by event')
ax.set_xlim((-2,np.max(replica_create)))
ax.set_ylabel('CDF')
ax.set_ylim((0,1.05))

ax.plot(bin_edges, cumsum)

# %%
