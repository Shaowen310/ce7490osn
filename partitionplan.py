import numpy as np


class PartitionPlan:
    NOALLOC = 0
    PRIMARY = 1
    REPLICA = 2

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
        # user partition assignment matrix, 0: none, 1: primary, 2: replica
        self.u2p = np.full((self.ucap, self.pcap), self.NOALLOC, dtype=np.int8)

    def partition_least_masters(self):
        au2app = self.u2p[np.ix_(self.ualloc, self.palloc)] == self.PRIMARY
        master_count = np.sum(au2app, axis=0, dtype=np.int64)
        apidx = np.nonzero(self.palloc)[0]
        return apidx[np.argmin(master_count)]

    def contains_user(self, user_id):
        return self.ualloc[user_id]

    def contains_partition(self, partition_id):
        return self.palloc[partition_id]

    def _expand_user_capacity(self):
        pass

    def _expand_partition_capacity(self):
        pass

    def partition_add_master(self, partition_id, user_id):
        assert self.palloc[partition_id]
        self.u2p[user_id, partition_id] = self.PRIMARY
        self.ualloc[user_id] = True

    def partition_add_slave(self, partition_ids, user_id):
        assert np.alltrue(self.palloc[partition_ids])
        self.u2p[user_id, partition_ids] = self.REPLICA
        self.ualloc[user_id] = True

    def _remove_master(self, user_id):
        self.u2p[user_id] = np.where(self.u2p == self.PRIMARY, self.NOALLOC, self.u2p)

    def _partition_remove_master(self, partition_id, user_id):
        self.u2p[user_id, partition_id] = self.NOALLOC

    def partition_remove_master(self, partition_id, user_id):
        assert self.palloc[partition_id]
        self._partition_remove_master(partition_id, user_id)
        self.ualloc[user_id] = not np.alltrue(self.u2p[user_id] == self.NOALLOC)

    def partition_ids(self):
        return np.nonzero(self.palloc)[0]

    def partition_ids_not_master(self, user_id):
        return np.nonzero(self.palloc & (self.u2p[user_id] != self.PRIMARY))[0]

    def find_partition_having_master(self, user_id):
        return np.nonzero(self.u2p[user_id] == self.PRIMARY)[0][0]

    def find_partition_having_slave(self, user_id):
        return np.nonzero(self.u2p[user_id] == self.REPLICA)[0]

    def move_master(self, to_partition_id, user_id):
        assert self.palloc[to_partition_id]
        self._remove_master(user_id)
        self.u2p[user_id, to_partition_id] = self.PRIMARY
