import numpy as np
import copy

# redundancy
K = 2


def add_node(pplan, user):
    pid_least_masters = pplan.partition_least_masters()
    pplan.partition_add_master(pid_least_masters, user)
    pids = pplan.partition_ids_not_having_master(user)
    pids_slave = pids[np.random.choice(len(pids), K)]
    pplan.partition_add_slave(pids_slave, user)

    return pplan


def add_edge(pplan, user1, user2, G):
    scores = []
    ratios = []
    strategies = []
    # no movements
    pplan_nomovements = no_movement_of_master(pplan, user1, user2)
    scores.append(evaluate(pplan_nomovements))
    ratios.append(imbalance_ratio(pplan_nomovements))
    strategies.append(pplan_nomovements)

    # move user1 master to user2 server
    pplan_user1_to_user2 = move_master(pplan, user1, user2, G)
    scores.append(evaluate(pplan_user1_to_user2))
    ratios.append(imbalance_ratio(pplan_user1_to_user2))
    strategies.append(pplan_user1_to_user2)

    # move user2 master to user1 server
    pplan_user2_to_user1 = move_master(pplan, user2, user1, G)
    scores.append(evaluate(pplan_user2_to_user1))
    ratios.append(imbalance_ratio(pplan_user2_to_user1))
    strategies.append(pplan_user2_to_user1)

    sort_tuple = [(ratios[i], scores[i], strategies[i]) for i in range(3)]
    sort_tuple.sort(key=lambda x: (x[0], -x[1]))
    pplan = sort_tuple[0][2]

    return pplan


def rm_node(pplan, user, G):
    user_neighbors = G.find_neighbors(user)  # neighbors list

    for neighbor in user_neighbors:
        pplan = rm_edge(pplan, user, neighbor, G)

    pplan.partition_rm_master(user)
    pplan.partition_re_slave_all(user)

    return None


def rm_edge(pplan, user1, user2, G):
    user1_master_server = pplan.find_master_server(user1)  # is a number
    user2_master_server = pplan.find_master_server(user2)

    if remove_slave_replica(pplan, user1_master_server, user2, user1, G):
        pplan.partition_remove_slave(user1_master_server, user2)

    if remove_slave_replica(pplan, user2_master_server, user1, user2, G):
        pplan.partition_remove_slave(user2_master_server, user1)

    return pplan


def no_movement_of_master(pplan, user1, user2):
    pplan_tmp = copy.deepcopy(pplan)

    user1_master_server = pplan_tmp.find_partition_having_master(user1)  # is a number
    user2_master_server = pplan_tmp.find_partition_having_master(user2)
    user1_slave_server = pplan_tmp.find_partition_having_slave(user1)  # is a list
    user2_slave_server = pplan_tmp.find_partition_having_slave(user2)

    if user1_master_server not in user2_slave_server and user1_master_server != user2_master_server:
        pplan_tmp.partition_add_slave(user2_master_server, user1)

    if user2_master_server not in user1_slave_server and user2_master_server != user1_master_server:
        pplan_tmp.partition_add_slave(user1_master_server, user2)

    return pplan_tmp


def move_master(pplan, user1, user2, G):
    pplan_tmp = copy.deepcopy(pplan)

    user1_master_server = pplan_tmp.find_partition_having_master(user1)  # is a number
    user2_master_server = pplan_tmp.find_partition_having_master(user2)

    pplan_tmp.move_master(user2_master_server, user1)  # move user1 master to user2 master server
    pplan_tmp.partition_add_slave(user1_master_server, user1)  # create user1 slave on user1 old mater

    user1_neighbors = G.find_neighbors(user1)  # neighbors list

    for neighbor in user1_neighbors:
        pplan_tmp.partition_add_slave(user2_master_server, neighbor)  # create new replica of user1 neighbors in new
        # master if it is not ready in.

        if remove_slave_replica(pplan, user1_master_server, neighbor, user1, G):
            pplan_tmp.partition_remove_slave(user1_master_server, neighbor)

    return pplan_tmp


def evaluate(pplan):
    user_num = pplan.user_num()
    replica_num = pplan.replica_num()

    return replica_num / user_num


def imbalance_ratio(pplan):
    servers = pplan.servers()
    master_num = []
    for server in servers:
        tmp = pplan.find_master_in_partition(server)  # list
        master_num.append(len(tmp))
    return 1.0 * max(master_num) / min(master_num)


def remove_slave_replica(pplan, server, user, userdel, G):
    master_replicas = pplan.find_master_in_partition(server)
    user_neighbors = G.find_neighbors(user)

    user_serives = np.intersect1d(master_replicas, user_neighbors)

    user_serives_sub_userdel = np.setdiff1d(user_serives, np.array([userdel]))

    if len(user_serives_sub_userdel) == 0:
        return True
    else:
        return False
