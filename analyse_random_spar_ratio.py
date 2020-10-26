# %%
import numpy as np
from partitionplan import PartitionPlan
import re
import matplotlib.pyplot as plt

# %%
# FOLDERS = [
#     'facebook_new_4', 'facebook_new_8', 'facebook_new_16', 'facebook_new_64', 'facebook_new_128', 'facebook_new_256',
#     'facebook_new_512', 'facebook_random_4', 'facebook_random_8', 'facebook_random_16', 'facebook_random_64',
#     'facebook_random_128', 'facebook_random_256', 'facebook_random_512'
# ]

# FOLDERS_LF = ['lastfm_new_4', 'lastfm_new_8', 'lastfm_new_16', 'lastfm_new_64','lastfm_new_128', 'lastfm_new_256', 'lastfm_new_512', 'lastfm_random_4','lastfm_random_8', 'lastfm_random_16', 'lastfm_random_64',  'lastfm_random_128', 'lastfm_random_256', 'lastfm_random_512']

FOLDERS = [
    'facebook_new_K0_4', 'facebook_new_K0_8', 'facebook_new_K0_16', 'facebook_new_K0_64', 'facebook_new_K0_128',
    'facebook_new_K0_256', 'facebook_new_K0_512', 'facebook_random_K0_4', 'facebook_random_K0_8',
    'facebook_random_K0_16', 'facebook_random_K0_64', 'facebook_random_K0_128', 'facebook_random_K0_256',
    'facebook_random_K0_512'
]

FOLDERS_LF = [
    'lastfm_new_K0_4', 'lastfm_new_K0_8', 'lastfm_new_K0_16', 'lastfm_new_K0_64', 'lastfm_new_K0_128',
    'lastfm_new_K0_256', 'lastfm_new_K0_512', 'lastfm_random_K0_4', 'lastfm_random_K0_8', 'lastfm_random_K0_16',
    'lastfm_random_K0_64', 'lastfm_random_K0_128', 'lastfm_random_K0_256', 'lastfm_random_K0_512'
]

LABELS = ['4', '8', '16', '32', '64', '256', '512']


def avg_n_slave_replicas_per_user(FOLDERS):
    n_replicas_list = []
    avg_n_slave_replicas_per_user = []
    for FOLDER in FOLDERS:
        pp = PartitionPlan()
        pp.load(FOLDER)

        n_replicas = pp.num_replicas()
        n_replicas_list.append(n_replicas)

        avg_n_slave_replicas_per_user.append(n_replicas / np.count_nonzero(pp.ualloc) - 1)
    return np.array(avg_n_slave_replicas_per_user)


avg_n_slave_replicas_per_user_fb = avg_n_slave_replicas_per_user(FOLDERS)
avg_n_slave_replicas_per_user_lf = avg_n_slave_replicas_per_user(FOLDERS_LF)

avg_n_slave_replicas_per_user_fb = np.reshape(avg_n_slave_replicas_per_user_fb, (2, -1))
avg_n_slave_replicas_per_user_lf = np.reshape(avg_n_slave_replicas_per_user_lf, (2, -1))

# %%
rand_spar_ratio_fb = avg_n_slave_replicas_per_user_fb[1, :] / avg_n_slave_replicas_per_user_fb[0, :]
rand_spar_ratio_lf = avg_n_slave_replicas_per_user_lf[1, :] / avg_n_slave_replicas_per_user_lf[0, :]

x = np.arange(len(LABELS))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
line1 = ax.plot(rand_spar_ratio_fb, 'x-', label='Facebook')
line2 = ax.plot(rand_spar_ratio_lf, 'o-', label='LastFM')

ax.set_ylabel('RAND/SPAR')
ax.set_xticks(x)
ax.set_xticklabels(LABELS)
ax.set_ylim(bottom=0)
ax.legend()

plt.show()
# %%
