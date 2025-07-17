#!/bin/bash

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
until curl -f -s http://localhost:8083/connectors > /dev/null; do
    echo "Kafka Connect is not ready yet. Waiting..."
    sleep 5
done

echo "Kafka Connect is ready!"

# Create the Debezium PostgreSQL connector
echo "Creating Debezium PostgreSQL connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    localhost:8083/connectors/ \
    -d @debezium-postgres-connector.json

echo "Connector created! Check status with: curl localhost:8083/connectors/inventory-connector/status"
