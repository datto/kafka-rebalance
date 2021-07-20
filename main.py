#!/usr/bin/env python3
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

import argparse
from argparse import ArgumentDefaultsHelpFormatter
import json
from kafka import KafkaAdminClient
from lib.connections import fetch, gen_reassignment_file, exec_reassign
from lib.rebalance import plan, PlanSettings
import logging
import os
from pprint import pformat
import random

LOG = logging.getLogger(__name__)
SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))


def main():
    parser = argparse.ArgumentParser(
        formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "zookeeper_server",
        help="Kafka zookeeper server (<server:port>)")
    parser.add_argument(
        "bootstrap_server",
        help="Kafka bootstrap server (<server:port>)")
    parser.add_argument("-i", "--iterations", type=int, default=20,
                        help="Maximum number of partitions to move.")
    parser.add_argument(
        "-p",
        "--partition-percentage",
        type=float,
        default=90,
        help="Don't move partitions whose sizes are within this percent of each other, to avoid swapping similar-sized shards.")
    parser.add_argument(
        "-P",
        "--disk-percentage",
        type=float,
        default=10,
        help="Don't exchange between nodes whose sizes are within this many percentage points of each other.")
    parser.add_argument("-d", "--dry-run", action="store_true",
                        help="Don't perform moves, just plan")
    parser.add_argument("-v", "--verbose", action="count",
                        help="Verbose logging")
    parser.add_argument(
        "--net-throttle",
        type=int,
        default=40000000,
        help="Limit transfer between brokers by this amount, in bytes/sec")
    parser.add_argument(
        "--disk-throttle",
        type=int,
        default=200000000,
        help="Limit transfer between disks on the same brokers by this amount, in bytes/set")
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for rebalancing to finish. Default is to watch for results from remote host")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    if args.verbose and args.verbose >= 1:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger("kafka_rebalance").setLevel(logging.DEBUG)
        logging.getLogger("rebalance_core").setLevel(logging.DEBUG)

    if os.path.exists("{}/reassign.json".format(SCRIPTDIR)):
        LOG.info("Reassignment JSON exists. Is a reassignment already running?")
        exit(0)

    settings = PlanSettings(
        max_iters=args.iterations,
        node_percentage_threshold=args.disk_percentage,
        item_percentage_threshold=args.partition_percentage,
        swap=False,
    )

    kafka_admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_server)
    try:
        partitions, brokers = fetch(
            kafka_admin,
            disk_glob="/kafka/*",
            ssh_args={
                "user": "root"})
    finally:
        kafka_admin.close()

    disks = [disk for broker in brokers for disk in broker.disks]

    LOG.info("Begin planning")
    moving_partitions = plan(disks, settings)

    for replica in moving_partitions:
        LOG.info(
            "Moving {}-{} from {} to {}".format(
                replica.topic,
                replica.id,
                replica.initial_owner,
                replica.planned_owner))

    json_data = gen_reassignment_file(partitions, moving_partitions)
    LOG.info("JSON reassignment data: {}".format(pformat(json_data)))

    if args.dry_run:
        LOG.info("Dry run complete, run without -d/--dry-run to execute")
        exit(0)

    with open("{}/reassign.json".format(SCRIPTDIR), "w") as f_out:
        json.dump(json_data, f_out)

    work_broker = random.choice(brokers)
    if not exec_reassign(
            json_data,
            work_broker,
            args.zookeeper_server,
            args.net_throttle,
            args.disk_throttle,
            not args.no_wait):
        exit(1)

    try:
        os.unlink("{}/reassign.json".format(SCRIPTDIR))
    except Exception:
        pass


if __name__ == "__main__":
    main()
