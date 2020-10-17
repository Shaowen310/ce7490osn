import numpy as np


class PartitionPlan:
    def __init__(self, n_partitions):
        self.uid2us = {}
        self.pid2ps = {i:i for i in range(n_partitions)}
        # user's storage position to user id
        self.us2uid = np.empty((0), dtype=np.int64)
        # partition's storage position to partition id
        self.ps2pid = np.arange(n_partitions, dtype=np.int64)
        # user primary partition assignment matrix
        self.user2primary = np.empty((0, n_partitions), dtype=np.bool)
        # user replica partition assignment matrix
        self.user2replica = np.empty((0, n_partitions), dtype=np.bool)
        self.capacity = n_partitions

    def partition_least_masters(self):
        master_count = np.sum(self.user2primary, axis=0)
        master_count[self.ps2pid == -1] = np.nan
        return self.ps2pid[np.nanargmin(master_count)]

    def contains_user(self, user_id):
        return user_id in self.uid2us

    def contains_partition(self, partition_id):
        return partition_id in self.pid2ps

    def _expand_user_capacity(self):

    def _expand_partition_capacity(self):

    def partition_add_master(self, partition_id, user_id):
        us = self.uid2us[user_id]
        ps = self.pid2ps[partition_id]
        self.user2primary[us, ps] = True

    def partition_add_slave(self, partition_id, user_id):
        us = self.uid2us[user_id]
        ps = self.pid2ps[partition_id]
        self.user2replica[us, ps] = True

    def partition_ids(self):

        return list(range(len(self.partitions)))
