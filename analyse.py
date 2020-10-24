# %%
import numpy as np


# %%
def num_replicas(ualloc, palloc, u2p):
    return np.count_nonzero(u2p[np.ix_(ualloc, palloc)])


# %%
FOLDER = 'test_server512'

palloc = np.load(FOLDER + '/palloc.npy')
ualloc = np.load(FOLDER + '/ualloc.npy')
u2p = np.load(FOLDER + '/u2p.npy')

n_replicas = num_replicas(ualloc, palloc, u2p)

print('num_replicas: ', n_replicas)

# %%
