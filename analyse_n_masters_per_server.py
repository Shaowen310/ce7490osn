# %%
import numpy as np
from partitionplan import PartitionPlan
import re
import matplotlib.pyplot as plt

# %%
# FOLDERS = ['facebook_new_4', 'facebook_new_16', 'facebook_new_64', 'facebook_new_256']
# FOLDERS = ['facebook_new_8', 'facebook_new_32', 'facebook_new_128', 'facebook_new_512']
# FOLDERS = ['facebook_new_4','facebook_new_8','facebook_new_16', 'facebook_new_32','facebook_new_64', 'facebook_new_128','facebook_new_256', 'facebook_new_512']
# FOLDERS = ['facebook_new_4', 'facebook_new_8', 'facebook_new_16', 'facebook_new_32']
# FOLDERS = ['facebook_new_64', 'facebook_new_128', 'facebook_new_256', 'facebook_new_512']
# FOLDERS = ['facebook_new_K0_8', 'facebook_new_K0_32', 'facebook_new_K0_128', 'facebook_new_K0_512']
# FOLDERS = ['facebook_rmedge50_8', 'facebook_rmedge50_32', 'facebook_rmedge50_128', 'facebook_rmedge50_512']
FOLDERS = ['facebook_rmedge50_K0_8', 'facebook_rmedge50_K0_32', 'facebook_rmedge50_K0_128', 'facebook_rmedge50_K0_512']

LABELS = ['8','32','128','512']

n_replicas_list = []
avg_n_slave_replicas_per_user_list = []
n_masters_per_server_list = []

for FOLDER in FOLDERS:
    palloc = np.load(FOLDER + '/palloc.npy')
    ualloc = np.load(FOLDER + '/ualloc.npy')
    u2p = np.load(FOLDER + '/u2p.npy')

    pp = PartitionPlan()
    pp.load(FOLDER)

    n_replicas = pp.num_replicas()
    n_replicas_list.append(n_replicas)
    avg_n_slave_replicas_per_user_list.append(n_replicas / np.count_nonzero(ualloc) - 1)

    n_masters_per_server = pp.num_masters_per_partition()

    n_masters_per_server_list.append(n_masters_per_server)

# %%
fig, axs = plt.subplots(1, 4)
plts = []
# for j in range(2):
for i in range(4):
    plot = axs[i].imshow(np.expand_dims(n_masters_per_server_list[i], axis=1),
                        cmap="Blues",
                        interpolation=None,
                        vmin=0,
                        aspect='auto')
    plot.axes.set_xticklabels([])
    plot.axes.set_xlabel(LABELS[i])
    plts.append(plot)
    cbar = fig.colorbar(plot, ax=axs[i],orientation="vertical")
    cbar.ax.invert_yaxis()
fig.tight_layout(pad=0.5)

plt.show()

# %%
