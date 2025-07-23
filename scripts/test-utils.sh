#!/bin/bash

# Test Utilities: Common functions for Debezium test scripts
# This file contains shared functions used across multiple test scripts

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;94m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

success() {
    echo -e "${GREEN}[SUCCESS] $1${NC}"
}

warning() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

# Function to wait for user input
wait_for_user() {
    read -p "Press Enter to continue..."
}

# Function to show current database state
show_database_state() {
    local step_name="$1"
    log "=== DATABASE STATE: $step_name ==="

    log "Current customers table:"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT id, first_name, last_name, email, created_at
        FROM inventory.customers
        ORDER BY id;
    " -t

    log "Current products table:"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT id, name, description, price, quantity, created_at
        FROM inventory.products
        ORDER BY id;
    " -t

    log "Current orders table:"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT id, customer_id, product_id, quantity, total_amount, status, created_at
        FROM inventory.orders
        ORDER BY id;
    " -t
    echo ""
}

# Function to capture comprehensive replication slot information
capture_replication_slot_info() {
    local step_name="$1"
    log "=== REPLICATION SLOT INFO: $step_name ==="

    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            slot_name,
            plugin,
            slot_type,
            active,
            restart_lsn,
            confirmed_flush_lsn,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag_size,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS flush_lag_size
        FROM pg_replication_slots
        WHERE slot_name LIKE '%debezium%' OR slot_name LIKE '%inventory%';
    "

    log "Current WAL LSN position:"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT pg_current_wal_lsn() AS current_wal_lsn;
    "

    # Capture latest offset message from Kafka Connect
    log "Latest Debezium offset message:"
    if docker ps --format "table {{.Names}}" | grep -q "^kafka$"; then
        latest_offset=$(docker exec kafka kafka-console-consumer \
            --bootstrap-server localhost:29092 \
            --topic my_connect_offsets \
            --from-beginning \
            --timeout-ms 10000 \
            --property print.key=true \
            --property key.separator=" = " 2>/dev/null | tail -1 | grep "inventory-connector" || echo "No offset messages found")

        if [ "$latest_offset" != "No offset messages found" ] && [ -n "$latest_offset" ]; then
            echo "  $latest_offset"

            # Extract LSN values from the offset message for comparison
            lsn_proc=$(echo "$latest_offset" | sed -n 's/.*"lsn_proc":\([0-9]*\).*/\1/p')
            lsn_commit=$(echo "$latest_offset" | sed -n 's/.*"lsn_commit":\([0-9]*\).*/\1/p')
            lsn=$(echo "$latest_offset" | sed -n 's/.*"lsn":\([0-9]*\).*/\1/p')

            if [ -n "$lsn" ] || [ -n "$lsn_proc" ]; then
                log "LSN comparison (PostgreSQL vs Debezium offsets):"
                # Get current PostgreSQL LSN in decimal for comparison
                current_lsn_decimal=$(docker exec postgres psql -U postgres -d testdb -t -c "
                    SELECT ('x' || lpad(split_part(pg_current_wal_lsn()::text, '/', 1), 8, '0'))::bit(32)::bigint * 4294967296::bigint +
                           ('x' || lpad(split_part(pg_current_wal_lsn()::text, '/', 2), 8, '0'))::bit(32)::bigint;
                " | tr -d ' ')

                confirmed_lsn_decimal=$(docker exec postgres psql -U postgres -d testdb -t -c "
                    SELECT ('x' || lpad(split_part(confirmed_flush_lsn::text, '/', 1), 8, '0'))::bit(32)::bigint * 4294967296::bigint +
                           ('x' || lpad(split_part(confirmed_flush_lsn::text, '/', 2), 8, '0'))::bit(32)::bigint
                    FROM pg_replication_slots WHERE slot_name = 'debezium_slot';
                " | tr -d ' ')

                if [ -n "$current_lsn_decimal" ] && [ -n "$lsn" ]; then
                    # Get hex values for PostgreSQL LSNs
                    current_lsn_hex=$(docker exec postgres psql -U postgres -d testdb -t -c "SELECT pg_current_wal_lsn();" | tr -d ' ')
                    confirmed_lsn_hex=$(docker exec postgres psql -U postgres -d testdb -t -c "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'debezium_slot';" | tr -d ' ')

                    # Convert Debezium decimal LSNs to hex format for display
                    lsn_hex=$(printf "0/%X" $lsn)
                    [ -n "$lsn_proc" ] && lsn_proc_hex=$(printf "0/%X" $lsn_proc)
                    [ -n "$lsn_commit" ] && lsn_commit_hex=$(printf "0/%X" $lsn_commit)

                    echo "  PostgreSQL current WAL LSN: $current_lsn_decimal ($current_lsn_hex)"
                    echo "  PostgreSQL confirmed flush LSN: $confirmed_lsn_decimal ($confirmed_lsn_hex)"
                    [ -n "$lsn_proc" ] && echo "  Debezium offset lsn_proc: $lsn_proc ($lsn_proc_hex)"
                    [ -n "$lsn_commit" ] && echo "  Debezium offset lsn_commit: $lsn_commit ($lsn_commit_hex)"
                    echo "  Debezium offset lsn: $lsn ($lsn_hex)"

                    # Calculate differences
                    if [ -n "$confirmed_lsn_decimal" ] && [ "$confirmed_lsn_decimal" -gt 0 ]; then
                        lsn_diff=$((lsn - confirmed_lsn_decimal))
                        if [ $lsn_diff -gt 0 ]; then
                            echo "  → Debezium is $lsn_diff LSN units ahead of PostgreSQL confirmed position"
                        elif [ $lsn_diff -lt 0 ]; then
                            echo "  → PostgreSQL confirmed position is $((confirmed_lsn_decimal - lsn)) LSN units ahead"
                        else
                            echo "  → Debezium and PostgreSQL positions are synchronized"
                        fi
                    fi
                fi
            fi
        else
            echo "  No offset messages found (connector may not have started processing yet)"
        fi
    else
        warning "Kafka not available - cannot check offset messages"
    fi
    echo ""
}

# Function to capture Kafka topic status
capture_kafka_status() {
    local step_name="$1"
    log "=== KAFKA TOPIC STATUS: $step_name ==="

    # Check if Kafka is available
    if ! docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1; then
        warning "Kafka not available - cannot check topic status"
        return
    fi

    # Show topic message counts (high watermarks)
    log "Message counts in CDC topics:"
    for topic in customers products orders; do
        if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q "^$topic$"; then
            count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:29092 \
                --topic $topic \
                --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')
            echo "  $topic: $count messages"
        else
            echo "  $topic: topic not created yet"
        fi
    done
    echo ""
}

# Function to show detailed message correlation
show_message_correlation() {
    local step_name="$1"
    log "=== MESSAGE CORRELATION: $step_name ==="

    # Show recent messages with more detail
    for topic in customers products orders; do
        if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q "^$topic$"; then
            log "Recent $topic messages (with payload details):"
            docker exec kafka kafka-console-consumer \
                --bootstrap-server localhost:29092 \
                --topic $topic \
                --from-beginning \
                --timeout-ms 5000 \
                --property print.key=true \
                --property key.separator=" | " \
                --property print.timestamp=true 2>/dev/null | tail -10 || true
            echo ""
        fi
    done
}

# Function to check message counts in Kafka topics
check_message_counts() {
    log "Checking message counts in Kafka topics..."
    for topic in customers products orders; do
        if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q "^$topic$"; then
            count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:29092 \
                --topic $topic \
                --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')
            echo "  $topic: $count messages"
        else
            echo "  $topic: topic not created yet"
        fi
    done
}

# Function to capture Kafka consumer group offsets
capture_kafka_offsets() {
    local step_name="$1"
    log "=== KAFKA CONSUMER OFFSETS: $step_name ==="

    # Check if Kafka is available
    if ! docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1; then
        warning "Kafka not available - cannot check consumer offsets"
        return
    fi

    # Show topic high watermarks
    log "Topic high watermarks (latest offsets):"
    for topic in customers products orders; do
        if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q "^$topic$"; then
            hwm=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:29092 \
                --topic $topic \
                --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')
            echo "  $topic: $hwm"
        else
            echo "  $topic: topic not created yet"
        fi
    done
    echo ""
}

# Function to check if PostgreSQL container is running
check_postgres_running() {
    log "Checking if PostgreSQL container is running..."
    if docker ps --format "table {{.Names}}" | grep -q "^postgres$"; then
        success "PostgreSQL container is running"
        return 0
    else
        error "PostgreSQL container is not running"
        return 1
    fi
}

# Function to check if Kafka container is running
check_kafka_running() {
    log "Checking if Kafka container is running..."
    if docker ps --format "table {{.Names}}" | grep -q "^kafka$"; then
        success "Kafka container is running"
        return 0
    else
        error "Kafka container is not running"
        return 1
    fi
}

# Function to setup Kafka Connect
setup_kafka_connect() {
    if [ -f "./scripts/setup-connector.sh" ]; then
        bash ./scripts/setup-connector.sh >/dev/null 2>&1 || {
            error "Kafka Connect setup failed"
            return 1
        }
        success "Kafka Connect setup completed"
    else
        warning "setup-connector.sh not found, skipping Kafka Connect setup"
    fi
}

# Function to show active database connections
show_database_connections() {
    local step_name="$1"
    log "=== DATABASE CONNECTIONS: $step_name ==="
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            application_name,
            client_addr,
            client_hostname,
            state,
            backend_start,
            query_start,
            left(query, 50) as query_preview
        FROM pg_stat_activity
        WHERE datname = 'testdb'
        AND pid != pg_backend_pid()
        ORDER BY backend_start;
    "
    echo ""
}

# Function to check Kafka Connect status (basic version)
check_connect_status_basic() {
    log "Checking Kafka Connect status..."
    local attempts=0
    while [ $attempts -lt 12 ]; do
        connector_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.connector.state' 2>/dev/null)
        task_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.tasks[0].state' 2>/dev/null)

        if [ "$connector_state" = "RUNNING" ] && [ "$task_state" = "RUNNING" ]; then
            success "Connector and task are both RUNNING!"
            return 0
        else
            log "Connector state: $connector_state, Task state: $task_state (attempt $((attempts+1))/12)"
            if [ $attempts -lt 11 ]; then
                sleep 10
            fi
        fi
        ((attempts++))
    done

    warning "Connector did not reach RUNNING state within expected time"
    return 1
}

# Function to wait for connector failure (for testing failure scenarios)
wait_for_connector_failure() {
    log "Waiting for connector to detect failure and transition to FAILED state..."
    local attempts=0
    local max_attempts=30  # Wait up to 5 minutes (30 * 10 seconds)

    while [ $attempts -lt $max_attempts ]; do
        connector_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.connector.state' 2>/dev/null)
        task_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.tasks[0].state' 2>/dev/null)

        # Check if either connector or task has failed
        if [ "$connector_state" = "FAILED" ] || [ "$task_state" = "FAILED" ]; then
            warning "Connector has failed as expected due to connection issues"
            log "Final state - Connector: $connector_state, Task: $task_state"

            # Show detailed status for analysis
            log "Detailed connector status:"
            curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq '.' || true
            return 0

        # Also check for other error-indicating states
        elif [[ "$connector_state" =~ ^(UNASSIGNED|PAUSED)$ ]] || [[ "$task_state" =~ ^(UNASSIGNED|PAUSED)$ ]]; then
            warning "Connector is in error-related state: Connector=$connector_state, Task=$task_state"
            log "This may indicate connection issues (attempt $((attempts+1))/$max_attempts)"

        else
            log "Connector state: $connector_state, Task state: $task_state (attempt $((attempts+1))/$max_attempts) - waiting for failure..."
        fi

        if [ $attempts -lt $((max_attempts-1)) ]; then
            sleep 10
        fi
        ((attempts++))
    done

    warning "Connector did not transition to FAILED state within expected time"
    log "Final connector status after $max_attempts attempts:"
    curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq '.' || true
    return 1
}
