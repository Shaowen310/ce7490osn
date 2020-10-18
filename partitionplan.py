import numpy as np


class PartitionPlan:
    def __init__(self, n_partitions, user_capacity, partition_capacity):
        # Assumption: user_id determines the user's storage location
        # partition_id determines the partition's storage location
        self.ucap = user_capacity
        self.pcap = partition_capacity
        # user storage allocation
        self.ualloc = np.zeros((self.ucap), dtype=np.bool)
        # partition storage allocation
        self.palloc = np.zeros((self.pcap), dtype=np.bool)
        self.palloc[:n_partitions] = True
        # user primary partition assignment matrix
        self.user2primary = np.zeros((self.ucap, self.pcap), dtype=np.bool)
        # user replica partition assignment matrix
        self.user2replica = np.zeros((self.ucap, self.pcap), dtype=np.bool)

    def partition_least_masters(self):
        master_count = np.sum(self.user2primary, axis=0, dtype=np.int64)
        master_count[~self.palloc] = np.iinfo(master_count.dtype).max
        return np.argmin(master_count)

    def contains_user(self, user_id):
        return self.ualloc[user_id]

    def contains_partition(self, partition_id):
        return self.palloc[partition_id]

    def _expand_user_capacity(self):
        pass

    def _expand_partition_capacity(self):
        pass

    def partition_add_master(self, partition_id, user_id):
        self.ualloc[user_id] = True
        self.palloc[partition_id] = True
        self.user2primary[user_id, partition_id] = True

    def partition_add_slave(self, partition_ids, user_id):
        self.ualloc[user_id] = True
        self.palloc[partition_ids] = True
        self.user2replica[user_id, partition_ids] = True

    def partition_ids(self):
        return np.nonzero(self.palloc)[0]
