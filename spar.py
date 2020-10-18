import numpy as np

# redundancy
K = 2


def add_node(pplan, user):
    pid_least_masters = pplan.partition_least_masters()
    pplan.partition_add_master(pid_least_masters, user)
    pids = pplan.partition_ids()
    pids_slave = pids[np.random.choice(len(pids), K)]
    pplan.partition_add_slave(pids_slave, user)
