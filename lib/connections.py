# Copyright © 2020 Datto, Inc.
# Author: Alex Parrill <aparrill@datto.com>
# Author: John Seekins <jseekins@datto.com>
#
# Licensed under the GNU General Public License Version 3
# Fedora-License-Identifier: GPLv3+
# SPDX-2.0-License-Identifier: GPL-3.0+
# SPDX-3.0-License-Identifier: GPL-3.0-or-later
#
# kafka-rebalance is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# kafka-rebalance is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with kafka-rebalance.  If not, see <https://www.gnu.org/licenses/>.

from fabric import Connection
from io import StringIO
import itertools
import json
from lib.rebalance import Node as ReNode, Item as ReItem
import logging
from pprint import pformat
import random
import re
from shlex import quote
from time import sleep, gmtime, strftime

__version__ = '0.1.0'
__all__ = [
    "Broker",
    "Disk",
    "PartitionReplica",
]

LOG = logging.getLogger("kafka_rebalance")


class Broker:
    def __init__(self, id, host, port, ssh):
        self.id = id
        self.host = host
        self.port = port
        self.ssh = ssh
        self.disks = []

    def initial_partition_replicas(self):
        for disk in self.disks:
            for replica in disk.initial_items:
                yield replica

    def planned_partition_replicas(self):
        for disk in self.disks:
            for replica in disk.planned_items:
                yield replica

    def contains_partition(self, topic, partition_id):
        for replica in self.initial_partition_replicas():
            if replica.topic == topic and replica.id == partition_id:
                return True
        for replica in self.planned_partition_replicas():
            if replica.topic == topic and replica.id == partition_id:
                return True
        return False

    def fetch_disks(self, disk_glob):
        LOG.info("Fetching disk usage on {}".format(self.ssh.host))
        disk_query = "df -l --output=target,size,used | grep \"{}\"".format(disk_glob)
        LOG.debug("Disk query: {}".format(disk_query))
        out = self.ssh.run(
            disk_query,
            hide="stdout",
            in_stream=False).stdout
        LOG.debug("Mounts discovered: {}".format(out))
        disks = []
        for (i, line) in enumerate(out.splitlines()):
            if i == 0:
                # Skip header
                continue
            mounted_on, size, _used = line.split()
            disks.append(Disk(self, mounted_on, int(size)))
        self.disks = disks

    def __str__(self):
        return "Broker{}@{}".format(self.id, self.host)


class Disk(ReNode):
    def __init__(self, broker, mount_point, capacity):
        super().__init__(capacity)
        self.broker = broker
        if not mount_point.endswith("/"):
            mount_point = mount_point + "/"
        self.mount_point = mount_point

    def fetch_replicas(self, partitions):
        LOG.info(
            "Fetching partition usage in {}:{}".format(
                self.broker.ssh.host,
                self.mount_point))
        LOG.debug("checking disk usage on {}".format(self.mount_point))
        out = self.broker.ssh.run(
            "find " +
            quote(
                self.mount_point) +
            " -maxdepth 1 -type d -exec du -x -s {} \\;",
            hide="stdout",
            in_stream=False).stdout

        dirs = {}
        for line in out.splitlines():
            usage, dir = line.split()
            if dir == self.mount_point:
                continue
            elif dir.startswith(self.mount_point):
                dirname = dir[len(self.mount_point):]
            else:
                raise RuntimeError(
                    "Kafka dir {!r} is not prefixed with drive path {!r}".format(
                        dir, self.mount_point))
            dirs[dirname] = int(usage)

        replicas = []
        for (topic, partition), (leader, owning_brokers) in partitions.items():
            key = "{}-{}".format(topic, partition)
            if key not in dirs:
                continue

            usage = dirs[key]
            try:
                replica_id = owning_brokers.index(self.broker.id)
            except ValueError:
                LOG.warn(
                    "Dir for {} exists on broker {} but broker is not in the partition's replica list".format(
                        key,
                        self.broker))
                continue

            replicas.append(PartitionReplica(
                self,
                topic,
                partition,
                replica_id,
                leader == self.broker.id,
                usage
            ))

        self.initial_items = replicas

    def __str__(self):
        return "{}:{}".format(self.broker, self.mount_point)


class PartitionReplica(ReItem):
    def __init__(self, disk, topic, id, replica_id, is_leader, size):
        super().__init__(size, disk)
        self.topic = topic
        self.id = id
        self.replica_id = replica_id
        self.is_leader = is_leader

    def can_move_to(self, disk):
        if self.is_leader:
            # Don't want to move leaders while they are working
            return False

        if not super().can_move_to(disk):
            return False

        if disk.broker is not self.initial_owner.broker:
            # If moving across brokers, make sure the broker doesn't contain
            # a replica of it
            if disk.broker.contains_partition(self.topic, self.id):
                return False

        return True

    def __str__(self):
        return "{}-{}repl{}".format(self.topic, self.id, self.replica_id)


def fetch(kafka_admin, disk_glob, ssh_args):
    LOG.info("Fetching topics")
    raw_topics = kafka_admin.describe_topics()
    partitions = {}
    for raw_topic in raw_topics:
        if raw_topic["error_code"] != 0:
            raise RuntimeError("Kafka error: {!r}".format(raw_topic))
        if raw_topic["is_internal"]:
            continue
        for raw_partition in raw_topic["partitions"]:
            if raw_partition["error_code"] != 0:
                raise RuntimeError("Kafka error: {!r}".format(raw_partition))
            partitions[(raw_topic["topic"], raw_partition["partition"])] = (
                raw_partition["leader"], raw_partition["replicas"])

    LOG.info("Fetching broker info")
    brokers = []
    for broker in kafka_admin.describe_cluster()["brokers"]:
        LOG.debug("Collecting data on {}".format(broker))
        broker = Broker(
            broker["node_id"],
            broker["host"],
            broker["port"],
            Connection(broker["host"], **ssh_args)
        )
        broker.fetch_disks(disk_glob)
        for disk in broker.disks:
            disk.fetch_replicas(partitions)
        brokers.append(broker)

    return (partitions, brokers)


def _find_new_position(replicas, new_id, new_position, old_id):
    if old_id in replicas:
        # if the old replica is in the list, well..just leave it alone
        return replicas.index(old_id)
    if replicas.count(new_id) < 2:
        # if we only have one position, just don't put the old replica in that position
        positions = [x for x, item in enumerate(replicas) if x != new_position]
        return random.choice(positions)
    if new_id not in replicas and old_id not in replicas:
        # if neither item is in the list (how?!) we'll just pick a position
        return 0
    """
    default assumption is two instances of the new item and no
    instances of the old id. We pick a location that isn't the new
    location but _is_ the new value.
    """
    positions = [x for x, item in enumerate(replicas) if item == new_id and x != new_position]
    return random.choice(positions)


def gen_reassignment_file(partitions, moved_replicas):
    new_assignments = {}
    for item in moved_replicas:
        id_name = "{}-{}".format(item.topic, item.id)
        _, initial_replica_nodes = partitions[(item.topic, item.id)]
        if id_name not in new_assignments:
            """
            Create a new object for moving...
            start with all partitions/log dirs in their "current" locations
            and simply update those than need to change
            We'll also potentially move the "old" replica off a node
            to ensure replicas are balanced across nodes as well as disks
            log_dirs can be largely left as "any" except when enforcing
            a disk-level move, so we won't try and track those
            """
            replicas = list(initial_replica_nodes)
            old_replica = replicas[item.replica_id]
            replicas[item.replica_id] = item.planned_owner.broker.id
            old_replica_pos = _find_new_position(replicas, item.planned_owner.broker.id,
                                                 item.replica_id, old_replica)
            replicas[old_replica_pos] = old_replica

            log_dirs = ["any"] * len(initial_replica_nodes)
            log_dirs[item.replica_id] = item.planned_owner.mount_point.rstrip("/")
            new_assignments[id_name] = {
                "topic": item.topic,
                "partition": item.id,
                "replicas": replicas,
                "original_replicas": list(initial_replica_nodes),
                "log_dirs": log_dirs,
            }
        else:
            old_replica = new_assignments[id_name]["replicas"][item.replica_id]
            new_assignments[id_name]["replicas"][item.replica_id] = item.planned_owner.broker.id
            old_replica_pos = _find_new_position(new_assignments[id_name]["replicas"], item.planned_owner.broker.id,
                                                 item.replica_id, old_replica)
            replicas[old_replica_pos] = old_replica
            new_assignments[id_name]["log_dirs"][item.replica_id] = item.planned_owner.mount_point.rstrip("/")
    """
    An example add:
    {'bfy-cass-use1-app-6': {'log_dirs': ['any', 'any', '/kafka/1'],
                             'partition': 6,
                             'replicas': [1, 0, 2],
                             'topic': 'bfy-cass-use1-app'},
     'opentsdb-metrics-10': {'log_dirs': ['any', 'any', '/kafka/0'],
                             'partition': 10,
                             'replicas': [1, 0, 2],
                             'topic': 'opentsdb-metrics'},
     'prometheus-dcs-production-4': {'log_dirs': ['any', '/kafka/5', 'any'],
                                     'partition': 4,
                                     'replicas': [1, 2, 0],
                                     'topic': 'prometheus-dcs-production'},
     'saas-use1-sys-0': {'log_dirs': ['any', 'any', '/kafka/0'],
                         'partition': 0,
                         'replicas': [0, 1, 2],
                         'topic': 'saas-use1-sys'},
     'storagenode-use1-sys-1': {'log_dirs': ['any', '/kafka/5', 'any'],
                                'partition': 1,
                                'replicas': [1, 2, 0],
                                'topic': 'storagenode-use1-sys'},
     'swift-cluster-sys-0': {'log_dirs': ['any', 'any', '/kafka/1'],
                             'partition': 0,
                             'replicas': [0, 1, 2],
                             'topic': 'swift-cluster-sys'},
     'unclassified-sys-1': {'log_dirs': ['any', '/kafka/2', 'any'],
                            'partition': 1,
                            'replicas': [0, 2, 1],
                            'topic': 'unclassified-sys'}}
    """
    LOG.debug("Added replica data to be rebalanced: {}".format(pformat(new_assignments)))

    json_items = []
    for partition_data in new_assignments.values():
        # if there are duplicates in the replica set list, we shouldn't process the move
        if len(partition_data["replicas"]) == len(set(partition_data["replicas"])):
            json_items.append({
                "topic": partition_data["topic"],
                "partition": partition_data["partition"],
                "replicas": partition_data["replicas"],
                "log_dirs": partition_data["log_dirs"],
            })

    return {
        "version": 1,
        "partitions": json_items
    }


def exec_reassign(
    json_data,
    work_broker,
    zk_server,
    net_throttle,
    disk_throttle,
    wait=True
):
    if len(json_data["partitions"]) < 1:
        LOG.error("Cannot process reassignmentfile with empty partitions section: {}".format(pformat(json_data)))
        return False
    json_file = StringIO()
    json.dump(json_data, json_file)

    ssh = work_broker.ssh

    filename = "/tmp/kafka-reassignment-{}.json".format(
        strftime("%Y.%m.%d.%H.%M.%S", gmtime()))

    ssh.put(json_file, filename)
    LOG.info("Added {} to remote host for execution".format(filename))
    cmdline = "/opt/kafka/bin/kafka-reassign-partitions.sh" + \
        " --bootstrap-server " + quote("{}:{}".format(work_broker.host, work_broker.port)) + \
        " --zookeeper " + quote(zk_server) + \
        " --reassignment-json-file " + quote(filename)
    exec_cmdline = cmdline + \
        " --throttle " + str(net_throttle) + \
        " --replica-alter-log-dirs-throttle " + str(disk_throttle) + \
        " --execute"
    verify_cmdline = cmdline + \
        " --verify"

    LOG.info("Submitting rebalance")
    exec_output = ssh.run(exec_cmdline, in_stream=False)

    if re.search(
            r"\b[a-zA-Z0-9_-]+Exception\b",
            exec_output.stdout) or re.search(
            r"\b[a-zA-Z0-9_-]+Exception\b",
            exec_output.stderr):
        LOG.warn(
            "Exception while starting partition reassignment. Some partitions may not get reassigned.")

    finished_failures = 0
    if wait:
        for spinner_char in itertools.cycle("/-\\|"):
            verify_output = ssh.run(
                verify_cmdline, in_stream=False, hide="both")
            still_running = [line for line in verify_output.stdout.split("\n") if "in progress" in line]
            print("\n{}\n{} partitions still processing".format("\n".join(still_running), len(still_running)))
            if "in progress" in verify_output.stdout:
                # Not done yet, keep waiting
                sleep(60)
                finished_failures = 0
            else:
                if "failed" in verify_output.stdout:
                    # Kafka reports failed at the end then flips to succeed
                    # when I run it manually
                    finished_failures += 1
                    if finished_failures >= 5:
                        break
                else:
                    break

        if "failed" in verify_output.stdout:
            LOG.warn(
                "One or more partitions or replicas failed to move. Output:\n{}".format(
                    verify_output))
            return False
        else:
            return True
