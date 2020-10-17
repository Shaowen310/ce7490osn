from partition import Partition

class PartitionPlan:
    def __init__(self):
        # make sure the array is dense
        self.partitions = []

    def __init__(self, capacity):
        self.partitions = [Partition(i) for i in range(capacity)]

    def partition_least_masters(self):
        if len(self.partitions) == 0:
            return None

        least_masters = 0
        p_id_least_masters = 0
        
        for p in self.partitions:
            num_masters = p.num_masters()
            if num_masters > least_masters:
                least_masters = num_masters
                p_id_least_masters = p.id

        return p_id_least_masters

    def get_partition(self, partition_id):
        return self.partitions[partition_id]

    def partition_add_master(self, partition_id, user):
        self.partitions[partition_id].add_master(user)
