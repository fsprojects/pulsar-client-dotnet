#!/usr/bin/env bash

bin/apply-config-from-env.py conf/client.conf

sleep 5

echo "Creating namespaces..."
bin/pulsar-admin namespaces create public/retention
bin/pulsar-admin namespaces create public/deduplication

echo "Setting namespaces settings..."
bin/pulsar-admin namespaces set-retention public/retention --time 3h --size 1G
bin/pulsar-admin namespaces set-deduplication public/deduplication --enable
bin/pulsar-admin namespaces set-schema-validation-enforce --enable public/default

echo "Creating partitioned topics..."
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned --partitions 3
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned2 --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned3 --partitions 3
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned4 --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned5 --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned6 --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned-dl-test --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/deduplication/partitioned --partitions 3

echo "Initializing transaction coordinator metadata..."
bin/pulsar initialize-transaction-coordinator-metadata -cs standalone:2181 -c standalone --initial-num-transaction-coordinators 2

echo "Init Standalone completed!"
