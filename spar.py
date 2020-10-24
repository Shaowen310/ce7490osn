import numpy as np
import copy
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


def add_edge(pplan, user1, user2, G, undirected):
    scores = []
    ratios = []
    strategies = []
    s_name = []

    user1_master_server = pplan.find_partition_having_master(
        user1)  # is a number
    user2_master_server = pplan.find_partition_having_master(user2)
    user1_slave_server = pplan.find_partition_having_slave(
        user1)  # is a list
    user2_slave_server = pplan.find_partition_having_slave(user2)

    furture = False

    if user1_master_server not in user2_slave_server and user1_master_server != user2_master_server:
        furture = True

    if undirected:
        if user2_master_server not in user1_slave_server and user2_master_server != user1_master_server:
            furture = True

    if furture:
        pplan_nomovements = no_movement_of_master(pplan, user1, user2, undirected)
        scores.append(evaluate(pplan_nomovements))
        ratios.append(imbalance_ratio(pplan_nomovements))
        strategies.append(pplan_nomovements)
        s_name.append('no movement')
        # print('no_movement plan', pplan_nomovements.u2p)

        # move user1 master to user2 server
        pplan_user1_to_user2 = move_master(pplan, user1, user2, G)
        scores.append(evaluate(pplan_user1_to_user2))
        ratios.append(imbalance_ratio(pplan_user1_to_user2))
        strategies.append(pplan_user1_to_user2)
        s_name.append('move u1 to u2')
        # print('move u1 to u2 plan', pplan_user1_to_user2.u2p)

        # move user2 master to user1 server
        pplan_user2_to_user1 = move_master(pplan, user2, user1, G)
        scores.append(evaluate(pplan_user2_to_user1))
        ratios.append(imbalance_ratio(pplan_user2_to_user1))
        strategies.append(pplan_user2_to_user1)
        s_name.append('move u2 to u1')
        # print('move u2 to u1 plan', pplan_user2_to_user1.u2p)

        sort_tuple = [(ratios[i], scores[i], strategies[i], s_name[i]) for i in range(3)]
        sort_tuple.sort(key=lambda x: (x[0], x[1]))

        # print(sort_tuple)
        pplan = sort_tuple[0][2]
        print(sort_tuple[0][3])

        return pplan
    else:
        return pplan


def add_server_1(pplan, new_server, G):
    servers = pplan.partition_ids()
    current_server_num = len(servers)
    user_num = pplan.user_num()

    avg_num = 1.0 * user_num / (current_server_num + 1)

    total_processed = 0
    for i in range(len(servers)):
        masters = pplan.find_master_in_partition(servers[i])
        masters_num = len(masters)
        move_num = masters_num - round(avg_num) + round(avg_num * i -
                                                        total_processed)
        if i == len(servers) - 1:
            move_num = user_num - total_processed
        total_processed = total_processed + move_num

        move_ids = np.random.choice(len(masters), move_num, replace=False)

        for id in move_ids:
            pplan.move_master_to_partition(new_server, masters[id],k=K)

            neighbors = G.get_neighbors(masters[id])  # neighbors list

            for neighbor in neighbors:
                if remove_slave_replica(pplan, servers[i], neighbor,
                                        masters[id], G):
                    pplan.partition_remove_slave(servers[i], neighbor)
    return pplan


def add_server_2(pplan):
    # server list add 1
    return pplan


def rm_server(pplan, serverldel, G):
    servers = pplan.partition_ids()
    current_server_num = len(servers)
    cap = current_server_num  # allow maximum imbalance

    masters = pplan.find_master_in_partition(serverldel)

    masters_connect = [len(G.get_neighbor(i)) for i in masters]

    sort_tuple = [(masters_connect[i], masters[i]) for i in range(len(masters))]
    sort_tuple.sort(key=lambda x: (-x[0]))

    for j in sort_tuple:
        id = j[1]

        # find the server hold most neighbors of this master
        user_neighbors = G.get_neighbors(id)  # neighbors list
        all_nerighbor_server = [np.append(pplan.find_partition_having_slave(i), pplan.find_partition_having_master(i))
                                for i in user_neighbors]

        all_nerighbor_server = np.array([item for sub_list in all_nerighbor_server for item in sub_list])

        all_nerighbor_server_num = np.bincount(all_nerighbor_server)

        max_neightbor_server = np.argsort(all_nerighbor_server_num)[-len(all_nerighbor_server_num)][::-1]
        master_num = pplan.num_masters_per_partition()

        min_num = np.min(master_num)
        for index in max_neightbor_server:

            cur_master_num = master_num[index]

            if cur_master_num + 1 - min_num < cap:
                pplan.move_master_to_partition(index, id,k=K)
                for neighbor in user_neighbors:
                    pplan.partition_add_slave(index, neighbor)
                break


def rm_node(pplan, user, G):
    user_neighbors = G.get_neighbors(user)  # neighbors list

    for neighbor in user_neighbors:
        pplan = rm_edge(pplan, user, neighbor, G)

    G.remove_node(user)

    pplan.partition_remove_master(user)
    pplan.partition_remove_slave_all(user)

    return pplan


def rm_edge(pplan, user1, user2, G):
    user1_master_server = pplan.find_master_server(user1)  # is a number
    user2_master_server = pplan.find_master_server(user2)

    if remove_slave_replica(pplan, user1_master_server, user2, user1, G):
        pplan.partition_remove_slave(user1_master_server, user2, k=K)

    if remove_slave_replica(pplan, user2_master_server, user1, user2, G):
        pplan.partition_remove_slave(user2_master_server, user1, k=K)

    return pplan


def no_movement_of_master(pplan, user1, user2, undirected=True):
    pplan_tmp = copy.deepcopy(pplan)

    user1_master_server = pplan_tmp.find_partition_having_master(
        user1)  # is a number
    user2_master_server = pplan_tmp.find_partition_having_master(user2)
    user1_slave_server = pplan_tmp.find_partition_having_slave(
        user1)  # is a list
    user2_slave_server = pplan_tmp.find_partition_having_slave(user2)

    if user1_master_server not in user2_slave_server and user1_master_server != user2_master_server:
        pplan_tmp.partition_add_slave(user1_master_server, user2)

    if undirected:
        if user2_master_server not in user1_slave_server and user2_master_server != user1_master_server:
            pplan_tmp.partition_add_slave(user2_master_server, user1)

    return pplan_tmp


def move_master(pplan, user1, user2, G):
    pplan_tmp = copy.deepcopy(pplan)
    # print("*********************************")
    # print(user1,user2)

    user1_master_server = pplan_tmp.find_partition_having_master(
        user1)  # is a number
    user2_master_server = pplan_tmp.find_partition_having_master(user2)

    # if user1_master_server != user2_master_server:

    pplan_tmp.move_master_to_partition(
        user2_master_server, user1,k=K)  # move user1 master to user2 master server

    user1_neighbors = G.get_neighbors(user1)  # neighbors list

    have_created = False
    for neighbor in user1_neighbors:

        if not have_created and pplan_tmp.find_partition_having_master(neighbor) == user1_master_server:
            pplan_tmp.partition_add_slave(
                user1_master_server, user1)  # create user1 slave on user1 old mater
            have_created = True

        pplan_tmp.partition_add_slave(
            user2_master_server,
            neighbor)  # create new replica of user1 neighbors in new
        # master if it is not ready in.

        if remove_slave_replica(pplan_tmp, user1_master_server, neighbor,
                                user1, G):
            pplan_tmp.partition_remove_slave(user1_master_server, neighbor)

    # print(pplan.u2p)
    # print(pplan_tmp.u2p)
    #
    # print("*****************************************")

    return pplan_tmp


def evaluate(pplan):
    user_num = pplan.num_users()
    replica_num = pplan.num_replicas()
    return replica_num / user_num - 1


def imbalance_ratio(pplan):
    master_num = pplan.num_masters_per_partition()
    max_num, min_num = np.max(master_num), np.min(master_num)
    return 1e38 if min_num == 0 else 1. * max_num / min_num


def remove_slave_replica(pplan, server, user, userdel, G):
    num_slave_replicas = pplan.num_slaves_by_user(user)

    if num_slave_replicas <= K:
        return False

    master_replicas = pplan.find_master_in_partition(server)

    user_neighbors = G.get_neighbors(user)

    user_serives = np.intersect1d(master_replicas, user_neighbors)

    user_serives_sub_userdel = np.setdiff1d(user_serives, np.array([userdel]))

    if len(user_serives_sub_userdel) == 0:
        return True
    else:
        return False
