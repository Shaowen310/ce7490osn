import numpy as np
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
    u1_master_server = pplan.find_partition_having_master(user1)
    u2_master_server = pplan.find_partition_having_master(user2)
    u1_slave_server = pplan.find_partition_having_slave(user1)
    u2_slave_server = pplan.find_partition_having_slave(user2)

    if u1_master_server not in u2_slave_server:
        pplan.partition_add_slave(u1_master_server, user2)
        remove_redundant_slaves_for_user(pplan, user2, G)

    if u2_master_server not in u1_slave_server:
        pplan.partition_add_slave(u2_master_server, user1)
        remove_redundant_slaves_for_user(pplan, user1, G)
    return pplan


def rm_edge(pplan, user1, user2, G):
    u1master = pplan.find_master_server(user1)
    u2master = pplan.find_master_server(user2)

    if remove_slave_replica(pplan, user2, u1master, G):
        pplan.partition_remove_slave(u1master, user2)

    if remove_slave_replica(pplan, user1, u2master, G):
        pplan.partition_remove_slave(u2master, user1)

    return pplan


def rm_node(pplan, user, G):
    user_neighbors = G.get_neighbors(user)  # neighbors list

    for neighbor in user_neighbors:
        pplan = rm_edge(pplan, user, neighbor, G)

    pplan.partition_remove_master(user)
    pplan.partition_remove_slave_all(user)

    return pplan


def remove_slave_replica(pplan, user, server, G):
    num_slave_replicas = pplan.num_slaves_by_user(user)
    return num_slave_replicas > K and is_slave_redundant(pplan, user, server, G)


def is_slave_redundant(pplan, user, server, G):
    neighbors = G.get_neighbors(user)

    for neighbor in neighbors:
        if pplan.find_partition_having_master(neighbor) == server:
            return False
    return True


def remove_redundant_slaves_for_user(pplan, user, G, k=K):
    user_slave_servers = pplan.find_partition_having_slave(user)
    n_slave_replicas = len(user_slave_servers)

    if n_slave_replicas <= k:
        return

    user_neighbors = G.get_neighbors(user)
    neighbor_master_servers = []
    for neighbor in user_neighbors:
        neighbor_master_server = pplan.find_partition_having_master(neighbor)
        neighbor_master_servers.append(neighbor_master_server)
    neighbor_master_servers = np.unique(np.array(neighbor_master_servers))

    slave_removal_condidates = np.setdiff1d(user_slave_servers, neighbor_master_servers)

    n_slaves_to_remove = min(n_slave_replicas - k, len(slave_removal_condidates))
    if n_slaves_to_remove > 0:
        slaves_to_remove = slave_removal_condidates[:n_slaves_to_remove]
        for slave in slaves_to_remove:
            pplan.partition_remove_slave(slave, user, k=0)
