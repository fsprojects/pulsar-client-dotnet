#!/usr/bin/env bash

bin/apply-config-from-env.py conf/client.conf

sleep 5

bin/pulsar-admin tenants create public
bin/pulsar-admin namespaces create public/default
