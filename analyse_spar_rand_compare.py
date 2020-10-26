# %%
import numpy as np
from partitionplan import PartitionPlan
import re
import matplotlib.pyplot as plt

# %%
# FOLDERS = ['facebook_new_4', 'facebook_new_8', 'facebook_new_16', 'facebook_new_64','facebook_new_128', 'facebook_new_256', 'facebook_new_512', 'facebook_random_4','facebook_random_8', 'facebook_random_16', 'facebook_random_64',  'facebook_random_128', 'facebook_random_256', 'facebook_random_512']

# FOLDERS = ['lastfm_new_4', 'lastfm_new_8', 'lastfm_new_16', 'lastfm_new_64','lastfm_new_128', 'lastfm_new_256', 'lastfm_new_512', 'lastfm_random_4','lastfm_random_8', 'lastfm_random_16', 'lastfm_random_64',  'lastfm_random_128', 'lastfm_random_256', 'lastfm_random_512']

# FOLDERS = ['facebook_new_K0_4', 'facebook_new_K0_8', 'facebook_new_K0_16', 'facebook_new_K0_64','facebook_new_K0_128', 'facebook_new_K0_256', 'facebook_new_K0_512', 'facebook_random_K0_4','facebook_random_K0_8', 'facebook_random_K0_16', 'facebook_random_K0_64',  'facebook_random_K0_128', 'facebook_random_K0_256', 'facebook_random_K0_512']

FOLDERS = ['lastfm_new_K0_4', 'lastfm_new_K0_8', 'lastfm_new_K0_16', 'lastfm_new_K0_64','lastfm_new_K0_128', 'lastfm_new_K0_256', 'lastfm_new_K0_512', 'lastfm_random_K0_4','lastfm_random_K0_8', 'lastfm_random_K0_16', 'lastfm_random_K0_64',  'lastfm_random_K0_128', 'lastfm_random_K0_256', 'lastfm_random_K0_512']

LABELS = ['4', '8', '16', '32', '64', '256', '512']

n_replicas_list = []
avg_n_slave_replicas_per_user_list = []

for FOLDER in FOLDERS:
    pp = PartitionPlan()
    pp.load(FOLDER)

    n_replicas = pp.num_replicas()
    n_replicas_list.append(n_replicas)

    avg_n_slave_replicas_per_user_list.append(n_replicas / np.count_nonzero(pp.ualloc) - 1)

avg_n_slave_replicas_per_user_list = np.array(avg_n_slave_replicas_per_user_list)

avg_n_slave_replicas_per_user_list = np.reshape(avg_n_slave_replicas_per_user_list, (2,-1))

# %%
x = np.arange(len(LABELS))  # the label locations
width = 0.35  # the width of the bars

spar = avg_n_slave_replicas_per_user_list[0]
rand = avg_n_slave_replicas_per_user_list[1]

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, spar, width, label='SPAR')
rects2 = ax.bar(x + width/2, rand, width, label='Random')

ax.set_ylabel('Replication overhead')
ax.set_xticks(x)
ax.set_xticklabels(LABELS)
ax.legend()

plt.show()

# %%
