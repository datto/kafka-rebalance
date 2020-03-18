Kafka disk balancer
===================

Automatically moves partition replicas across disks and brokers using the `kafka-reassign-partitions.sh` binary, to balance out disk usage and improve utilization.

Installation
------------

This is a standard poetry project. Install poetry to your system with `pip3 install poetry`.
Setup the venv with `poetry install`. Run with `poetry run python3 -m kafka_rebalance`.

Notes
-----

pip (or your system packager) may not create a symlink binary for poetry. If this happens (`which poetry` fails), you can _also_ execute poetry commands with `python3 -m poetry`.
