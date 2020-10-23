# %%
import numpy as np
import snap
from graph import Graph
from partitionplan import PartitionPlan
import spar

action = [
    0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8, 0, 9, 0, 10, 0, 11, 0, 12,
    0, 13, 0, 14, 0, 15, 0, 16, 0, 17, 0, 18, 0, 19, 0, 20
]


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
def load_graph(graph: Graph, action_file, undirected=False):
    for (n1, n2) in generate_action(action_file):
        graph.add_node(n1)
        graph.add_node(n2)
        graph.add_edge(n1, n2)
        if undirected:
            graph.add_edge(n2, n1)


# %%
pp = PartitionPlan(20, 5000, 10)

pp.partition_ids()
G1 = snap.TNGraph.New()
G = Graph(G1)


# %%
# for i in range(len(action) - 1):
#     node1 = action[i]
#     node2 = action[i + 1]
#     if not pp.contains_user(node1):
#         pp = spar.add_node(pp, node1)
#         G.add_node(node1)
#     if not pp.contains_user(node2):
#         pp = spar.add_node(pp, node2)
#         G.add_node(node2)
#     G.add_edge(node1, node2)
#     pp = spar.add_edge(pp, node1, node2, G)

# %%
def partitaion(save):
    global pp
    for (node1, node2) in generate_action('./data/snap/facebook/facebook_combined/facebook_combined_rand.txt'):
        if not pp.contains_user(node1):
            pp = spar.add_node(pp, node1)
            G.add_node(node1)
        if not pp.contains_user(node2):
            pp = spar.add_node(pp, node2)
            G.add_node(node2)
        G.add_edge(node1, node2)
        G.add_edge(node2, node1)
        pp = spar.add_edge(pp, node1, node2, G)

    pp.save(save)


def load(load_name):
    global pp
    pp.load(load_name)
    load_graph(G, './data/snap/facebook/facebook_combined/facebook_combined_rand.txt', undirected=True)


if __name__ == '__main__':
    partitaion('./test_server10')
    # load()
    # print(pp.u2p)
