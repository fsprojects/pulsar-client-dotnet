#!/usr/bin/env bash

bin/apply-config-from-env.py conf/client.conf

sleep 5

bin/pulsar-admin namespaces create public/default
bin/pulsar-admin namespaces create public/retention
bin/pulsar-admin namespaces set-retention public/retention --time 3h --size 1G
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned --partitions 3
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned2 --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned3 --partitions 3

