import numpy as np
import copy

# redundancy
K = 2


def add_node(pplan, user):
    pid_least_masters = pplan.partition_least_masters()
    pplan.partition_add_master(pid_least_masters, user)
    pids = pplan.partition_ids_not_master(user)
    pids_slave = pids[np.random.choice(len(pids), K)]
    pplan.partition_add_slave(pids_slave, user)


def add_edge(pplan, user1, user2, G):
    # no movements
    pplan_nomovements = no_movement_of_master(pplan, user1, user2)

    # move user1 master to user2 server

    return None


def rm_node(pplan, user):
    return None


def rm_edge(pplan, user1, user2):
    return None


def no_movement_of_master(pplan, user1, user2):
    pplan_tmp = copy.deepcopy(pplan)

    user1_master_server = pplan_tmp.find_master_server(user1)  # is a number
    user2_master_server = pplan_tmp.find_master_server(user2)
    user1_slave_server = pplan_tmp.find_slave_server(user1)  # is a list
    user2_slave_server = pplan_tmp.find_slave_server(user2)

    if user1_master_server not in user2_slave_server and user1_master_server != user2_master_server:
        pplan_tmp.partition_add_slave(user2_master_server, user1)

    if user2_master_server not in user1_slave_server and user2_master_server != user1_master_server:
        pplan_tmp.partition_add_slave(user1_master_server, user2)


    return pplan_tmp


def move_master(pplan, user1, user2, G):
    pplan_tmp = copy.deepcopy(pplan)

    user1_master_server = pplan_tmp.find_master_server(user1)  # is a number
    user2_master_server = pplan_tmp.find_master_server(user2)
    user1_slave_server = pplan_tmp.find_slave_server(user1)  # is a list
    user2_slave_server = pplan_tmp.find_slave_server(user2)

    pplan_tmp.move_master_server(user2_master_server, user1)  # move user1 master to user2 master server
    pplan_tmp.partition_add_slave(user1_master_server, user1)  # create user1 slave on user1 old mater

    user1_neighbors = G.find_neighbors(user1)  # neighbors list

    for neighbor in user1_neighbors:
        pplan_tmp.partition_add_slave(user2_master_server, neighbor)  # create new replica of user1 neighbors in new
        # master if it is not ready in.
        pplan_tmp.partition_remove_slave(user1_master_server, neighbor)  # remove all slave of user1 nrighbors from
        # user1 old master, and must not less than K

    master_replicas = pplan_tmp.find_master_replica(user1_master_server)
    for master in master_replicas:
        neighbors = G.find_neighbors(master)  # neighbors list
        for neighbor in neighbors:
            pplan_tmp.partition_add_slave(user1_master_server, neighbor)


def evaluate(pplan):
    user_num = pplan.user_num()
    replica_num = pplan.replica_num()

    return replica_num / user_num

def imbalance_ratio(pplan):
