# %%
import numpy as np
import snap
from graph import Graph
from partitionplan import PartitionPlan
import spar
import sys


# import randalloc as spar

# action = [
#     0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8, 0, 9, 0, 10, 0, 11, 0, 12,
#     0, 13, 0, 14, 0, 15, 0, 16, 0, 17, 0, 18, 0, 19, 0, 20
# ]


def generate_action(file_name):
    record = []
    with open(file_name, 'r') as file:
        for line in file.readlines():
            line = line.replace('\n', '')
            if line != '':
                user_ids = line.split(' ')
                user1 = int(user_ids[0])
                user2 = int(user_ids[1])
                record.append((user1, user2))
    return record


# generate a random action file for further usage, call this method only when you first run this method
def random_action(file_name):
    record = []
    with open(file_name, 'r') as file:
        for line in file.readlines():
            line = line.replace('\n', '')
            if line != '':
                record.append(line)

    rand_id = np.random.choice(len(record), len(record), replace=False)

    file_name_random = file_name[0:-4] + str('_rand.txt')
    write_back = ''
    for it_id in rand_id:
        write_back = write_back + record[it_id] + '\n'

    with open(file_name_random, 'w+') as file:
        file.write(write_back)


# random_action('./data/snap/facebook/facebook_combined/facebook_combined.txt')


# if load pplan, you need also load the graph, call this function when you call pp.load(*args)
def load_graph(graph: Graph, action_file):
    for (n1, n2) in generate_action(action_file):
        graph.add_node(n1)
        graph.add_node(n2)
        graph.add_edge(n1, n2)


def partition(save, file):
    global pp
    for (node1, node2) in generate_action(file):
        if not pp.contains_user(node1):
            pp = spar.add_node(pp, node1)
            G.add_node(node1)
        if not pp.contains_user(node2):
            pp = spar.add_node(pp, node2)
            G.add_node(node2)
        G.add_edge(node1, node2)
        pp = spar.add_edge(pp, node1, node2, G)

    pp.save(save)


def load(load_name, file):
    global pp
    pp.load(load_name)
    load_graph(G, file)


def preprocess_twitter(file_name):
    record = {}
    index = 0
    save = ''

    with open(file_name, 'r') as file:
        for line in file.readlines():
            line = line.replace('\n', '')
            if line != '':
                user_ids = line.split(' ')
                user1 = int(user_ids[0])
                user2 = int(user_ids[1])
                if user1 not in record.keys():
                    record[user1] = index
                    index = index + 1
                if user2 not in record.keys():
                    record[user2] = index
                    index = index + 1
            save = save + str(record[user1]) + ' ' + str(record[user2]) + '\n'

    print(index)

    file_name_random = file_name[0:-4] + str('_rand.txt')
    with open(file_name_random, 'w+') as file:
        file.write(save)


def remove_edge(file):
    global pp
    user_num = pp.num_users()
    replica_num = pp.num_replicas()
    #
    # print(user_num)
    # print(replica_num)
    actions = generate_action(file)

    edge_num = len(actions)

    # print(edge_num)

    k = edge_num

    for (node1, node2) in actions:
        G.remove_edge(node1, node2)
        pp = spar.rm_edge(pp, node1, node2, G)
        # print(pp.num_users())
        print(pp.num_replicas())
        k = k - 1

        if k == int(edge_num * 0.5):
            pp.save('facebook_rmedge50_K0_' + str(k_num))


if __name__ == '__main__':

    k_num = int(sys.argv[1])
    # num = 256
    for server_num in [k_num]:
        pp = PartitionPlan(server_num, 5000, server_num)

        pp.partition_ids()
        G1 = snap.TUNGraph.New()
        G = Graph(G1)
        # partition('./twitter_new_' + str(server_num), './data/snap/twitter/twitter_combined_rand.txt')
        load('./facebook_new_K0_' + str(server_num),
             './data/snap/facebook/facebook_combined/facebook_combined_rand.txt')
        remove_edge('./data/snap/facebook/facebook_combined/facebook_combined_rand.txt')
