import random

# redundancy
K = 2


def add_node(pplan, user):
    pid_least_masters = pplan.partition_least_masters()
    pplan.partition_add_master(pid_least_masters, user)
    pids = pplan.partition_ids()

    for _ in range(K):
        pid_slave = random.choice(pids)
        pplan.partition_add_slave(pid_slave, user)
