#!/usr/bin/env bash

bin/apply-config-from-env.py conf/client.conf

sleep 5

echo "Creating 'standalone' cluster..."
bin/pulsar-admin clusters create standalone --broker-url pulsar://localhost:6650 --url http://localhost:8080

echo "Creating 'public' tenant..."
bin/pulsar-admin tenants create public

echo "Creating 'default' namespace..."
bin/pulsar-admin namespaces create public/default

echo "Init Standalone TLS completed!"
