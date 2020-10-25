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


def add_edge(pplan, user1, user2, G):
    u1_master_server = pplan.find_partition_having_master(user1)
    u2_master_server = pplan.find_partition_having_master(user2)
    u1_slave_servers = pplan.find_partition_having_slave(user1)
    u2_slave_servers = pplan.find_partition_having_slave(user2)

    if u1_master_server == u2_master_server:
        return pplan

    if (u1_master_server in u2_slave_servers) and (u2_master_server in u1_slave_servers):
        return pplan

    scores = []
    # ratios = []
    strategies = []
    s_name = []

    current_replica = evaluate(pplan)

    pp_nomove, n_replica_created = no_movement_of_master(pplan, user1, user2, G)
    # nomove_replica = evaluate(pp_nomove)

    scores.append(evaluate(pp_nomove))
    # ratios.append(imbalance_ratio(pp_nomove))
    strategies.append(pp_nomove)
    s_name.append('no movement, replica create {0}'.format(n_replica_created))
    # print('no_movement plan', pplan_nomovements.u2p)

    # move user1 master to user2 master server
    pp_u1_to_u2, n_replica_created = move_master(pplan, user1, user2, G)
    u1_to_u2_replica = evaluate(pplan)
    scores.append(evaluate(pp_u1_to_u2))
    # ratios.append(imbalance_ratio(pp_u1_to_u2))
    strategies.append(pp_u1_to_u2)
    s_name.append('move u1 to u2, replica create {0}'.format(n_replica_created))
    # print('move u1 to u2 plan', pplan_user1_to_user2.u2p)

    # move user2 master to user1 server
    pp_u2_to_u1, n_replica_created = move_master(pplan, user2, user1, G)
    u2_to_u1_replica = evaluate(pplan)
    scores.append(evaluate(pp_u2_to_u1))
    # ratios.append(imbalance_ratio(pp_u2_to_u1))
    strategies.append(pp_u2_to_u1)
    s_name.append('move u2 to u1, replica create {0}'.format(n_replica_created))
    # print('move u2 to u1 plan', pplan_user2_to_user1.u2p)
    if scores[0] <= scores[1] and scores[0] <= scores[2]:
        print(s_name[0])
        return pp_nomove

    sort_tuple = [(scores[i], strategies[i], s_name[i]) for i in range(3)]
    sort_tuple.sort(key=lambda x: (x[0]))

    masters1 = pplan.find_masters_in_partition(u1_master_server)
    masters2 = pplan.find_masters_in_partition(u2_master_server)

    masters1_num = len(masters1)
    masters2_num = len(masters2)

    if sort_tuple[0][1] == pp_u1_to_u2:
        if masters1_num > masters2_num or (sort_tuple[1][0] - sort_tuple[0][0]) > 3*masters2_num/masters1_num:
            print(sort_tuple[0][2])
            return sort_tuple[0][1]
        else:
            print(sort_tuple[1][2])
            return sort_tuple[1][1]

    if sort_tuple[0][1] == pp_u2_to_u1:
        if masters2_num > masters1_num or (sort_tuple[1][0] - sort_tuple[0][0]) > 3*masters1_num/masters2_num:
            print(sort_tuple[0][2])
            return sort_tuple[0][1]
        else:
            print(sort_tuple[1][2])
            return sort_tuple[1][1]

    print(sort_tuple[0][2])
    return sort_tuple[0][1]


def add_server_1(pplan, new_server, G):
    servers = pplan.partition_ids()
    current_server_num = len(servers)
    user_num = pplan.user_num()

    avg_num = 1.0 * user_num / (current_server_num + 1)

    total_processed = 0
    for i in range(len(servers)):
        masters = pplan.find_masters_in_partition(servers[i])
        masters_num = len(masters)
        move_num = masters_num - round(avg_num) + round(avg_num * i - total_processed)
        if i == len(servers) - 1:
            move_num = user_num - total_processed
        total_processed = total_processed + move_num

        move_ids = np.random.choice(len(masters), move_num, replace=False)

        for id in move_ids:
            pplan.move_master_to_partition(new_server, masters[id], k=K)

            neighbors = G.get_neighbors(masters[id])  # neighbors list

            for neighbor in neighbors:

                pplan.partition_add_slave(new_server, neighbor)
                remove_redundant_slaves_for_user(pplan, neighbor, G)

                if remove_slave_replica(pplan, servers[i], neighbor, masters[id], G):
                    pplan.partition_remove_slave(servers[i], neighbor)
    return pplan


def add_server_2(pplan):
    # server list add 1
    return pplan


def rm_server(pplan, serverldel, G):
    servers = pplan.partition_ids()
    current_server_num = len(servers)
    cap = current_server_num  # allow maximum imbalance

    masters = pplan.find_masters_in_partition(serverldel)

    masters_connect = [len(G.get_neighbor(i)) for i in masters]

    sort_tuple = [(masters_connect[i], masters[i]) for i in range(len(masters))]
    sort_tuple.sort(key=lambda x: (-x[0]))

    for j in sort_tuple:
        id = j[1]

        # find the server hold most neighbors of this master
        user_neighbors = G.get_neighbors(id)  # neighbors list
        all_nerighbor_server = [
            np.append(pplan.find_partition_having_slave(i), pplan.find_partition_having_master(i))
            for i in user_neighbors
        ]

        all_nerighbor_server = np.array([item for sub_list in all_nerighbor_server for item in sub_list])

        all_nerighbor_server_num = np.bincount(all_nerighbor_server)

        max_neightbor_server = np.argsort(all_nerighbor_server_num)[-len(all_nerighbor_server_num)][::-1]
        master_num = pplan.num_masters_per_partition()

        min_num = np.min(master_num)
        for index in max_neightbor_server:

            cur_master_num = master_num[index]

            if cur_master_num + 1 - min_num < cap:
                pplan.move_master_to_partition(index, id, k=K)
                for neighbor in user_neighbors:
                    pplan.partition_add_slave(index, neighbor)
                    remove_redundant_slaves_for_user(pplan, neighbor, G)
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


def no_movement_of_master(pplan, user1, user2, G):
    pptmp = copy.deepcopy(pplan)
    n_replica_created = 0

    u1_master_server = pptmp.find_partition_having_master(user1)
    u2_master_server = pptmp.find_partition_having_master(user2)
    u1_slave_server = pptmp.find_partition_having_slave(user1)
    u2_slave_server = pptmp.find_partition_having_slave(user2)

    if u1_master_server not in u2_slave_server:
        pptmp.partition_add_slave(u1_master_server, user2)
        n_replica_created += 1
        remove_redundant_slaves_for_user(pptmp, user2, G)

    if u2_master_server not in u1_slave_server:
        pptmp.partition_add_slave(u2_master_server, user1)
        n_replica_created += 1
        remove_redundant_slaves_for_user(pptmp, user1, G)

    return pptmp, n_replica_created


def move_master(pplan, user1, user2, G):
    pptmp = copy.deepcopy(pplan)

    n_replica_created = 0

    u1_master_server = pptmp.find_partition_having_master(user1)
    u2_master_server = pptmp.find_partition_having_master(user2)

    # if user1_master_server != user2_master_server:

    # move user1 master to user2 master server
    pptmp.move_master_to_partition(u2_master_server, user1, k=K)
    n_replica_created += 1

    user1_neighbors = G.get_neighbors(user1)  # neighbors list

    have_created = False
    for neighbor in user1_neighbors:
        if not have_created and pptmp.find_partition_having_master(neighbor) == u1_master_server:
            pptmp.partition_add_slave(u1_master_server, user1)  # create user1 slave on user1 old mater
            n_replica_created += 1
            remove_redundant_slaves_for_user(pptmp, user1, G)
            have_created = True

        # create new replica of user1 neighbors in new
        pptmp.partition_add_slave(u2_master_server, neighbor)
        n_replica_created += 1
        remove_redundant_slaves_for_user(pptmp, neighbor, G)
        # master if it is not ready in.

        if remove_slave_replica(pptmp, u1_master_server, neighbor, user1, G):
            pptmp.partition_remove_slave(u1_master_server, neighbor)

    return pptmp, n_replica_created


def evaluate(pplan):
    # user_num = pplan.num_users()
    replica_num = pplan.num_replicas()
    return replica_num


def imbalance_ratio(pplan):
    master_num = pplan.num_masters_per_partition()
    max_num, min_num = np.max(master_num), np.min(master_num)
    return 1. * (max_num + 1) / (min_num + 1)


def remove_slave_replica(pplan, server, user, userdel, G):
    num_slave_replicas = pplan.num_slaves_by_user(user)

    if num_slave_replicas <= K:
        return False

    master_users = pplan.find_masters_in_partition(server)
    user_neighbors = G.get_neighbors(user)
    user_serives = np.intersect1d(master_users, user_neighbors)
    user_serives_sub_userdel = np.setdiff1d(user_serives, np.array([userdel]))
    return len(user_serives_sub_userdel) == 0


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


def find_redundant_slaves_for_user(pplan, user, G):
    user_slave_servers = pplan.find_partition_having_slave(user)
    user_neighbors = G.get_neighbors(user)
    neighbor_master_servers = []
    for neighbor in user_neighbors:
        neighbor_master_server = pplan.find_partition_having_master(neighbor)
        neighbor_master_servers.append(neighbor_master_server)
    neighbor_master_servers = np.unique(np.array(neighbor_master_servers))
    return np.setdiff1d(user_slave_servers, neighbor_master_servers)


def is_slave_redundant(pplan, user, server, G):
    master_users = pplan.find_masters_in_partition(server)
    neighbors = G.get_neighbors(user)
    intersect = np.intersect1d(master_users, neighbors)
    return len(intersect) == 0
