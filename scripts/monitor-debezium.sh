#!/bin/bash

# Debezium Monitoring Script
# This script helps monitor the state of Debezium during error testing scenarios

echo "===================================================="
echo "           DEBEZIUM STATUS MONITOR"
echo "===================================================="

# Function to check if a service is running
check_service() {
    local service_name=$1
    local container_name=$2

    if docker ps --format "table {{.Names}}" | grep -q "^${container_name}$"; then
        echo "✅ ${service_name}: RUNNING"
        return 0
    else
        echo "❌ ${service_name}: STOPPED"
        return 1
    fi
}

# Function to check Kafka Connect status
check_kafka_connect() {
    echo ""
    echo "📡 KAFKA CONNECT STATUS:"
    echo "----------------------------------------"

    if check_service "Kafka Connect" "kafka-connect"; then
        # Check if Kafka Connect API is responding
        if curl -f -s http://localhost:8083/ > /dev/null; then
            echo "✅ Kafka Connect API: RESPONSIVE"

            # Check connector status
            echo ""
            echo "🔌 CONNECTOR STATUS:"
            connector_status=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null)
            if [ $? -eq 0 ]; then
                echo "$connector_status" | jq '.'
            else
                echo "❌ No connector found or API not responding"
            fi
        else
            echo "❌ Kafka Connect API: NOT RESPONDING"
        fi
    fi
}

# Function to check Kafka topics and offsets
check_kafka_topics() {
    echo ""
    echo "📋 KAFKA TOPICS & MESSAGES:"
    echo "----------------------------------------"

    if check_service "Kafka" "kafka"; then
        # List topics
        echo "Available topics:"
        docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -E "(customers|products|orders)"

        # Check message counts in each topic
        for topic in customers products orders; do
            echo ""
            echo "Topic: $topic"
            msg_count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:29092 \
                --topic $topic \
                --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')

            if [ "$msg_count" != "" ]; then
                echo "  Total messages: $msg_count"
            else
                echo "  Total messages: 0 or topic doesn't exist"
            fi
        done
    fi
}

# Function to check PostgreSQL replication slot
check_postgres_replication() {
    echo ""
    echo "🐘 POSTGRESQL REPLICATION STATUS:"
    echo "----------------------------------------"

    if check_service "PostgreSQL" "postgres"; then
        # Check replication slots
        echo "Replication slots:"
        docker exec postgres psql -U postgres -d testdb -c "SELECT slot_name, plugin, slot_type, database, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;" 2>/dev/null

        echo ""
        echo "WAL status:"
        docker exec postgres psql -U postgres -d testdb -c "SELECT pg_current_wal_lsn(), pg_current_wal_insert_lsn();" 2>/dev/null

        echo ""
        echo "Active connections:"
        docker exec postgres psql -U postgres -d testdb -c "SELECT application_name, client_addr, state, sent_lsn, write_lsn, flush_lsn, replay_lsn FROM pg_stat_replication;" 2>/dev/null
    fi
}

# Function to check Kafka Connect offsets
check_connect_offsets() {
    echo ""
    echo "📊 KAFKA CONNECT OFFSETS:"
    echo "----------------------------------------"

    if docker ps --format "table {{.Names}}" | grep -q "^kafka$"; then
        echo "Connect offset topic messages:"
        docker exec kafka kafka-console-consumer \
            --bootstrap-server localhost:29092 \
            --topic my_connect_offsets \
            --from-beginning \
            --timeout-ms 10000 \
            --property print.key=true \
            --property key.separator=" = " 2>/dev/null | tail -10
    fi
}

# Function to show recent CDC events
show_recent_events() {
    echo ""
    echo "🔄 RECENT CDC EVENTS (last 5 from each topic):"
    echo "----------------------------------------"

    for topic in customers products orders; do
        echo ""
        echo "Recent $topic events:"
        docker exec kafka kafka-console-consumer \
            --bootstrap-server localhost:29092 \
            --topic $topic \
            --max-messages 5 \
            --timeout-ms 10000 \
            --property print.key=true \
            --property key.separator=" = " 2>/dev/null | tail -5
    done
}

# Main execution
main() {
    # Check all services
    echo "🏥 SERVICE HEALTH CHECK:"
    echo "----------------------------------------"
    check_service "Zookeeper" "zookeeper"
    check_service "Kafka" "kafka"
    check_service "PostgreSQL" "postgres"
    check_service "Kafka Connect" "kafka-connect"
    check_service "Kafka UI" "kafka-ui"

    # Detailed checks
    check_kafka_connect
    check_kafka_topics
    check_postgres_replication

    # Option to show more details
    echo ""
    read -p "Show Connect offsets? (y/n): " show_offsets
    if [ "$show_offsets" = "y" ]; then
        check_connect_offsets
    fi

    echo ""
    read -p "Show recent CDC events? (y/n): " show_events
    if [ "$show_events" = "y" ]; then
        show_recent_events
    fi

    echo ""
    echo "===================================================="
    echo "Monitor completed. Kafka UI available at: http://localhost:8080"
    echo "===================================================="
}

# Execute main function
main
