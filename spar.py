# redundancy
K = 2

def add_node(pplan, user):
    p = pplan.partition_least_masters()
    