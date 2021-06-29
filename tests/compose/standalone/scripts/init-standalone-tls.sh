#!/usr/bin/env bash

bin/apply-config-from-env.py conf/client.conf

sleep 5

bin/pulsar-admin clusters create standalone --broker-url pulsar://localhost:6650 --url http://localhost:8080
bin/pulsar-admin tenants create public
bin/pulsar-admin namespaces create public/default
