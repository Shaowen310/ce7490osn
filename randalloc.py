from numpy.random import default_rng

RNG = default_rng(0)

# redundancy
K = 2


def add_node(pplan, user):
    pid_least_masters = pplan.partition_least_masters()
    pplan.partition_add_master(pid_least_masters, user)
    pids = pplan.partition_ids_not_having_master(user)
    pids_slave = pids[RNG.choice(len(pids), K, replace=False)]
    pplan.partitions_add_slave(pids_slave, user)

    return pplan


def add_edge(pplan, user1, user2, G):
    u1master = pplan.find_partition_having_master(user1)
    u2master = pplan.find_partition_having_master(user2)
    pplan.partition_add_slave(u1master, user2)
    pplan.partition_add_slave(u2master, user1)

    return pplan


def rm_edge(pplan, user1, user2, G):
    u1master = pplan.find_master_server(user1)  # is a number
    u2master = pplan.find_master_server(user2)

    if remove_slave_replica(pplan, user2):
        pplan.partition_remove_slave(u1master, user2)

    if remove_slave_replica(pplan, user1):
        pplan.partition_remove_slave(u2master, user1)

    return pplan


def rm_node(pplan, user, G):
    user_neighbors = G.get_neighbors(user)  # neighbors list

    for neighbor in user_neighbors:
        pplan = rm_edge(pplan, user, neighbor, G)

    pplan.partition_remove_master(user)
    pplan.partition_remove_slave_all(user)

    return pplan


def remove_slave_replica(pplan, user):
    num_slave_replicas = pplan.num_slaves_by_user(user)
    if num_slave_replicas + 1 <= K:
        return False
    return True
