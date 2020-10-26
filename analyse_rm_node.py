# %%
import numpy as np
from partitionplan import PartitionPlan
import re
import matplotlib.pyplot as plt
import pandas as pd

# %%
FILE = 'rmedgk0.csv'

df = pd.read_csv(FILE)

df -= 1


# %%
plt.style.use('seaborn-whitegrid')

fig, ax = plt.subplots()

linestyles = ['-','-','-','-','-','-','-','-']

for i in range(df.shape[1]):
    line = ax.plot(df.iloc[:,i], linestyles[i], label=df.columns[i])

ax.set_ylabel('Average number of slave replica per server')
ax.set_xlabel('Edge removal events')
ax.set_xlim(left=0)
ax.set_ylim(bottom=0)
# ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax.legend()

plt.show()


# %%
