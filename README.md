
# Kafka disk balancer

Automatically moves partition replicas across disks and brokers using the `kafka-reassign-partitions.sh` binary, to balance out disk usage and improve utilization.

When providing bootstrap hosts, it's strongly recommended you use IPs rather than DNS names. This helps avoid any DNS resolution problems during connections to different systems (zk/kafka).

# General program options

```
python3 main.py -h
usage: main.py [-h] [-i ITERATIONS] [-p PARTITION_PERCENTAGE] [-P DISK_PERCENTAGE] [-d] [-v]
               [--net-throttle NET_THROTTLE] [--disk-throttle DISK_THROTTLE] [-w]
               zookeeper_server bootstrap_server

positional arguments:
  zookeeper_server      Kafka zookeeper server (<ip:port>)
  bootstrap_server      Kafka bootstrap server (<ip:port>)

optional arguments:
  -h, --help            show this help message and exit
  -i ITERATIONS, --iterations ITERATIONS
                        Maximum number of partitions to move. (default: 10)
  -p PARTITION_PERCENTAGE, --partition-percentage PARTITION_PERCENTAGE
                        Don't move partitions whose sizes are within this percent of each other, to avoid
                        swapping similar-sized shards. (default: 90)
  -P DISK_PERCENTAGE, --disk-percentage DISK_PERCENTAGE
                        Don't exchange between nodes whose sizes are within this many percentage points of
                        each other. (default: 10)
  -d, --dry-run         Don't perform moves, just plan (default: False)
  -v, --verbose         Verbose logging (default: None)
  --net-throttle NET_THROTTLE
                        Limit transfer between brokers by this amount, in bytes/sec (default: 20000000)
  --disk-throttle DISK_THROTTLE
                        Limit transfer between disks on the same brokers by this amount, in bytes/set
                        (default: 200000000)
  -w, --wait            Wait for rebalancing to finish. Default is to return after starting transfer
                        (default: False)
```

Installation
------------

* Requirements (for debian)
  * python3-fabric
  * python3-decorator
  * python3-kafka

* Requirements (pip)
  * fabric
  * decorator
  * kafka-python

## Installing through pyenv

  * apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
xz-utils tk-dev libffi-dev liblzma-dev python-openssl git
  * curl https://pyenv.run | PYENV_ROOT=$(pwd)/.pyenv bash
  * PYENV_ROOT=$(pwd)/.pyenv .pyenv/bin/pyenv install 3.6.8
  * $(pwd)/.pyenv/versions/3.6.8/bin/pip install --upgrade pip
  * $(pwd)/.pyenv/versions/3.6.8/bin/pip install fabric decorator kafka-python

## Running with pyenv

`$(pwd)/.pyenv/versions/3.6.8/bin/python main.py`
