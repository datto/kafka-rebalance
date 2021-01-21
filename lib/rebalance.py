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


from abc import ABC
import statistics
import logging

LOG = logging.getLogger("rebalance_core")

__version__ = "0.1.0"


class Node(ABC):
    """
    Structure that can hold items (ex. a host or disk)
    """

    def __init__(self, capacity):
        self.capacity = capacity
        self.initial_items = []
        self.planned_items = None
        self.planned_used = None

    def planned_fraction_used(self):
        return self.planned_used / self.capacity

    def planned_sort(self):
        self.planned_items.sort(key=lambda i: i.size, reverse=True)
        self.planned_used = sum(v.size for v in self.planned_items)


class Item(ABC):
    """
    An item, owned by a node, that has a size and can be relocated to a
    different node.
    """

    def __init__(self, size, initial_owner=None):
        self.size = size
        self.initial_owner = initial_owner
        self.planned_owner = None

    @property
    def has_moved(self):
        return self.planned_owner is not None

    @property
    def current_owner(self):
        if self.planned_owner is not None:
            return self.planned_owner
        else:
            return self.initial_owner

    def can_move_to(self, node):
        """
        Tests if the item can be moved to a node.

        Default checks if node is the same as the initial owner and if the
        node has space for this item. Apply other checks here.
        """
        if node is self.initial_owner:
            return False
        if node.planned_used + self.size > node.capacity:
            return False
        return True


class PlanSettings:
    def __init__(self, **kwargs):
        self.max_iters = kwargs.pop("max_iters")
        self.node_fraction_threshold = kwargs.pop(
            "node_percentage_threshold") / 100.0
        if "item_percentage_threshold" in kwargs:
            self.item_fraction_threshold = 1 - kwargs.pop(
                "item_percentage_threshold") / 100.0
        else:
            self.item_fraction_threshold = None
        self.swap = kwargs.pop("swap", False)


def plan(nodes, settings):
    for node in nodes:
        node.planned_items = list(node.initial_items)
        for item in node.initial_items:
            if item.initial_owner is None:
                item.initial_owner = node
            elif item.initial_owner is not node:
                raise RuntimeError("initial_owner for {!r} is not {!r}")
            item.planned_owner = None

    plan_step = plan_step_swap if settings.swap else plan_step_move
    all_moves = []

    for _ in range(settings.max_iters):
        moved_items = plan_one(nodes, settings, plan_step)
        if moved_items:
            all_moves.extend(moved_items)
        else:
            LOG.info("No more moves possible, stopping")
            break
    return all_moves


def plan_one(nodes, settings, plan_step):
    resort(nodes)
    current_pvariance = percent_used_variance(nodes)

    for large_item in iter_large_items(settings, nodes):
        LOG.debug("Trying to move {}...".format(large_item))
        moved_items = plan_step(
            nodes, settings, current_pvariance, large_item)
        if moved_items is not None:
            return moved_items
    return None


def plan_step_swap(nodes, settings, current_pvariance, large_item):
    for small_item in iter_small_items(nodes, settings, large_item):
        # Is the "small" shard actually the smaller of the two?
        if small_item.store >= large_item.store:
            continue

        # Does the swap save enough space to be worth it?
        size_fraction = min(large_item.size, small_item.size) / \
            max(large_item.size, small_item.size)
        if size_fraction > settings.item_fraction_threshold:
            LOG.debug("\tItem sizes too similar")
            continue

        # Are the two nodes too similar in disk utilization?
        if abs(
            large_item.current_owner.planned_fraction_used() -
            small_item.current_owner.planned_fraction_used()
        ) < settings.node_fraction_threshold:
            LOG.debug("\tNode sizes too similar")
            continue

        # Can we move items onto new nodes?
        if not small_item.can_move_to(
            large_item.initial_owner
        ) or not large_item.can_move_to(small_item.initial_owner):
            LOG.debug("\tCan't move to node")
            continue

        # Does swapping result in a more balanced cluster?
        swapped_pvariance = percent_used_variance(
            nodes,
            exclude_items=[
                (large_item.initial_owner, large_item),
                (small_item.initial_owner, small_item),
            ],
            include_items=[
                (large_item.initial_owner, small_item),
                (small_item.initial_owner, large_item),
            ],
        )
        if swapped_pvariance >= current_pvariance:
            LOG.debug("\tNot more balanced")
            continue

        LOG.info(
            "Swapping {} with {}".format(
                large_item,
                small_item))
        move(large_item, small_item.current_owner)
        move(small_item, large_item.current_owner)
        return (large_item, small_item)
    return None


def plan_step_move(nodes, settings, current_pvariance, item):
    for node in reversed(nodes):
        LOG.debug("\t...onto {}".format(node))

        # Are the two nodes too similar in disk utilization?
        if abs(
            node.planned_fraction_used() -
            item.current_owner.planned_fraction_used()
        ) < settings.node_fraction_threshold:
            LOG.debug("\tNode sizes too similar")
            continue

        # Can we move this item to the new node?
        if not item.can_move_to(node):
            LOG.debug("\tCan't move to node")
            continue

        # Does moving result in a more balanced cluster?
        moved_pvariance = percent_used_variance(
            nodes,
            exclude_items=[(item.initial_owner, item)],
            include_items=[(node, item)],
        )
        if moved_pvariance >= current_pvariance:
            LOG.debug("\tNot more balanced")
            continue

        LOG.info("Moving {} to {}".format(item, node))
        move(item, node)
        return (item,)
    return None


def move(item, dest_node):
    src_node = item.current_owner
    src_node.planned_items.remove(item)
    item.planned_owner = dest_node
    dest_node.planned_items.append(item)


def resort(nodes):
    for node in nodes:
        node.planned_sort()
    nodes.sort(key=lambda n: n.planned_fraction_used(), reverse=True)


def percent_used_variance(nodes, include_items=[], exclude_items=[]):
    """
    Gets the variance of the percent storage used

    Optionally exclude or include items in the computation, for seeing if a
    configuration is better without actually committing to it
    """

    def percentage(node):
        the_sum = node.planned_used
        for exclude_node, exclude_item in exclude_items:
            if exclude_node is node:
                the_sum -= exclude_item.size
        for include_node, include_item in include_items:
            if include_node is node:
                the_sum += include_item.size

        return the_sum / node.capacity

    return statistics.pvariance(percentage(node) for node in nodes)


def iter_large_items(settings, nodes_by_size):
    """
    Yields large items that ought to be exchanged, with the highest-priority
    items first.

    Prioritize moving items off the most full hosts, with the largest items
    going first.
    """
    for node in nodes_by_size:
        if (
            abs(nodes_by_size[-1].planned_fraction_used() -
                node.planned_fraction_used()) <
            settings.node_fraction_threshold
        ):
            # Big node has a similar percent used than all of the smaller
            # nodes, so there's no way we can exchange anything. Stop.
            LOG.debug("Stopping at node {}, fract used is {}, within threshold of {}'s fraction used of {}".format(
                      node, node.planned_fraction_used(), nodes_by_size[-1], nodes_by_size[-1].planned_fraction_used()))
            break

        for item in node.planned_items:
            if item.has_moved:
                LOG.debug("{} has already moved".format(item))
                continue
            yield item


def iter_small_items(settings, nodes_by_size, large_node):
    """
    Yields small items that ought to be exchanged, with the highest-priority
    items first.

    Prioritizes the opposite way that iter_large_items does: starts with
    smallest items on most free hosts.
    """
    for node in reversed(nodes_by_size):
        if node is large_node:
            # We've met up with the large node. All other nodes will be bigger
            # than it, so there's no point in continuing
            break

        for item in reversed(node.planned_items):
            if item.has_moved:
                continue
            yield item


def format_bytes(num_bytes):
    """
    Formats a number into human-friendly byte units (KiB, MiB, etc)
    """
    if num_bytes >= 1024 * 1024 * 1024 * 1024:
        return "%.2fTiB" % (num_bytes / (1024 * 1024 * 1024 * 1024))
    if num_bytes >= 1024 * 1024 * 1024:
        return "%.2fGiB" % (num_bytes / (1024 * 1024 * 1024))
    if num_bytes >= 1024 * 1024:
        return "%.2fMiB" % (num_bytes / (1024 * 1024))
    if num_bytes >= 1024:
        return "%.2fKiB" % (num_bytes / (1024))
    return "%dB" % num_bytes
