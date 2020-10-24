import os

import numpy as np


class PartitionPlan:
    NOALLOC = 0
    MASTER = 1
    SLAVE = 2

    def __init__(self, ualloc, palloc, u2p):
        self.ucap = len(ualloc)
        self.pcap = len(palloc)
        self.ualloc = ualloc
        self.palloc = palloc
        self.u2p = u2p

    def __init__(self, n_partitions, user_capacity, partition_capacity):
        # Assumption: user_id determines the user's storage location
        # partition_id determines the partition's storage location
        self.ucap = user_capacity
        self.pcap = partition_capacity
        # user storage allocation
        self.ualloc = np.zeros(self.ucap, dtype=np.bool)
        # partition storage allocation
        self.palloc = np.zeros(self.pcap, dtype=np.bool)
        self.palloc[:n_partitions] = True
        # user partition assignment matrix, 0: none, 1: MASTER, 2: SLAVE
        self.u2p = np.full((self.ucap, self.pcap), self.NOALLOC, dtype=np.int8)

    def save(self, folder):

        if not os.path.exists(folder):
            os.makedirs(folder)

        ualloc_file = os.path.join(folder, 'ualloc.npy')
        np.save(ualloc_file, self.ualloc)
        palloc_file = os.path.join(folder, 'palloc.npy')
        np.save(palloc_file, self.palloc)
        u2p_file = os.path.join(folder, 'u2p.npy')
        np.save(u2p_file, self.u2p)

    def load(self, folder):
        try:
            ualloc_file = os.path.join(folder, 'ualloc.npy')
            self.ualloc = np.load(ualloc_file)
            palloc_file = os.path.join(folder, 'palloc.npy')
            self.palloc = np.load(palloc_file)
            u2p_file = os.path.join(folder, 'u2p.npy')
            self.u2p = np.load(u2p_file)

            self.ucap = len(self.ualloc)
            self.pcap = len(self.palloc)
        except FileNotFoundError:
            print('file can not be found')

    def num_masters_per_partition(self):
        au2app = self.u2p[np.ix_(self.ualloc, self.palloc)] == self.MASTER
        return np.sum(au2app, axis=0, dtype=np.int64)

    def partition_least_masters(self):
        master_count = self.num_masters_per_partition()
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
        if self.u2p[user_id, partition_id] != self.NOALLOC:
            print('Add master ignored as user {0} partition {1} is allocated'.
                  format(user_id, partition_id))
            return
        self.u2p[user_id, partition_id] = self.MASTER
        self.ualloc[user_id] = True

    def partition_add_slave(self, partition_id, user_id):
        assert self.palloc[partition_id]
        if self.u2p[user_id, partition_id] != self.NOALLOC:
            print('Add slave ignored as user {0} partition {1} is allocated'.
                  format(user_id, partition_id))
            return
        self.u2p[user_id, partition_id] = self.SLAVE
        self.ualloc[user_id] = True

    def partitions_add_slave(self, partition_ids, user_id):
        for pid in partition_ids:
            self.partition_add_slave(pid, user_id)

    def _remove_master(self, user_id):
        self.u2p[user_id,
                 np.flatnonzero(
                     self.u2p[user_id] == self.MASTER)] = self.NOALLOC

    def _partition_remove_replica(self, partition_id, user_id):
        self.u2p[user_id, partition_id] = self.NOALLOC

    def partition_remove_slave(self, partition_id, user_id, k=2):
        assert self.palloc[partition_id]
        if self.u2p[user_id, partition_id] != self.SLAVE:
            print(
                'Remove slave ignored as user {0} partition {1} is not slave'.format(
                    user_id, partition_id))
            return
        if self.num_slaves_by_user(user_id) <= k:
            print('Remove slave ignored as at least {0} slave should be kept'.format(k))
            return
        self._partition_remove_replica(partition_id, user_id)
        self.ualloc[user_id] = not np.alltrue(
            self.u2p[user_id] == self.NOALLOC)

    def remove_user(self, user_id):
        self.u2p[user_id] = np.full(self.u2p.shape[1],
                                    self.NOALLOC,
                                    dtype=np.int8)
        self.ualloc[user_id] = False

    def partition_ids(self):
        return np.flatnonzero(self.palloc)

    def partition_ids_not_having_master(self, user_id):
        return np.flatnonzero(self.palloc & (self.u2p[user_id] != self.MASTER))

    def partition_ids_not_having_replica(self, user_id):
        return np.flatnonzero(self.palloc & (self.u2p[user_id] == self.NOALLOC))

    def find_partition_having_master(self, user_id):
        return np.flatnonzero(self.u2p[user_id] == self.MASTER)[0]

    def find_partition_having_slave(self, user_id):
        return np.flatnonzero(self.u2p[user_id] == self.SLAVE)

    def move_master_to_partition(self, to_partition_id, user_id):
        assert self.palloc[to_partition_id]
        self._remove_master(user_id)
        add_slave = self.u2p[user_id, to_partition_id] == self.SLAVE
        self.u2p[user_id, to_partition_id] = self.MASTER
        if add_slave:
            pids = self.partition_ids_not_having_replica(user_id)
            if len(pids) == 0:
                print('No partition available to assign new slave for user {0}'.format(user_id))
                return
            self.u2p[user_id, pids[0]] = self.SLAVE

    def find_master_in_partition(self, partition_id):
        assert self.palloc[partition_id]
        return np.flatnonzero(self.u2p[:, partition_id] == self.MASTER)

    def num_slaves_by_user(self, user_id):
        return np.count_nonzero(self.u2p[user_id] == self.SLAVE)

    def num_users(self):
        return np.count_nonzero(self.ualloc)

    def num_replicas(self):
        return np.count_nonzero(self.u2p)

    def num_replicas(self):
        return np.count_nonzero(self.u2p[np.ix_(self.ualloc, self.palloc)])
