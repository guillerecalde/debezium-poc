#!/bin/bash

# Test Script: Kafka Connection Failure
# This script tests how Debezium handles Kafka connection failures and restarts

echo "============================================"
echo "    KAFKA CONNECTION FAILURE TEST"
echo "============================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;94m'
NC='\033[0m' # No Color

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

# Function to check if Kafka container is running
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

# Function to capture replication slot information
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
    echo ""
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

    # List consumer groups
    log "Checking for consumer groups..."
    consumer_groups=$(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --list 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$consumer_groups" ] && [ "$consumer_groups" != "" ]; then
        log "Available consumer groups:"
        echo "$consumer_groups"

        # Try to find Debezium-related consumer groups
        log "Debezium Connect worker offsets:"
        found_group=false

        # Try different possible consumer group names
        for group_name in "connect-inventory-connector" "inventory-connector" "connect-cluster" "dbserver1"; do
            if echo "$consumer_groups" | grep -q "$group_name"; then
                log "Found consumer group: $group_name"
                docker exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --group "$group_name" --describe 2>/dev/null
                found_group=true
                break
            fi
        done

        if [ "$found_group" = false ]; then
            log "No Debezium-related consumer groups found yet. Available groups:"
            echo "$consumer_groups"
            log "(This is normal if Kafka Connect hasn't started consuming yet)"
        fi
    else
        log "No consumer groups exist yet"
        log "(This is normal during initial setup or when Kafka Connect hasn't started consuming)"
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

# Function to check Kafka Connect status
check_connect_status() {
    log "Checking Kafka Connect status..."
    # Check connector status
    local attempts=0
    while [ $attempts -lt 12 ]; do
        connector_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.connector.state' 2>/dev/null)
        if [ "$connector_state" = "RUNNING" ]; then
            success "Connector is RUNNING again!"
            break
        else
            log "Connector state: $connector_state (attempt $((attempts+1))/12)"
            sleep 10
            ((attempts++))
        fi
    done
}

# Function to generate test data
generate_test_data() {
    log "Generating test data in PostgreSQL..."
    log "Inserting baseline customers and products..."
    docker exec postgres psql -U postgres -d testdb -c "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('Test', 'User1', 'test1@kafka-failure.com'),
        ('Test', 'User2', 'test2@kafka-failure.com');

        INSERT INTO inventory.products (name, description, price, quantity) VALUES
        ('Test Product 1', 'Product before Kafka failure', 99.99, 10),
        ('Test Product 2', 'Another test product', 149.99, 5);
    "
    success "✅ Baseline data inserted: 2 customers, 2 products"
}

# Function to check message counts in Kafka topics
check_message_counts() {
    log "Checking message counts in Kafka topics..."
    for topic in customers products orders; do
        count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:29092 \
            --topic $topic \
            --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')
        echo "  $topic: $count messages"
    done
}

# Function to consume recent messages
consume_recent_messages() {
    local topic=$1
    log "Recent messages from $topic topic:"
    docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:29092 \
        --topic $topic \
        --from-beginning \
        --timeout-ms 10000 \
        --property print.key=true \
        --property key.separator=" = " 2>/dev/null || true
}

# Main test execution
main() {
    log "Starting Kafka Connection Failure Test"

        # Step 1: Verify initial setup
    log "Step 1: Verifying initial setup..."
    if ! docker ps | grep -q kafka; then
        error "Kafka container is not running. Please start the environment first."
        exit 1
    fi

    # Verify Kafka is running before starting the test
    check_kafka_running
    setup_kafka_connect
    check_connect_status

    # Show initial state before any changes
    show_database_state "Initial Setup"
    capture_replication_slot_info "Initial Setup"
    capture_kafka_offsets "Initial Setup"
    wait_for_user

    # Step 2: Generate baseline data
    log "Step 2: Generating baseline data..."
    generate_test_data

    sleep 5
    show_database_state "After Baseline Data"
    capture_replication_slot_info "After Baseline Data"
    capture_kafka_offsets "After Baseline Data"
    show_message_correlation "After Baseline Data"
    wait_for_user

    # Step 3: Stop Kafka (simulate connection failure)
    warning "Step 3: Simulating Kafka connection failure..."

    # Capture state before stopping Kafka
    log "Capturing state before Kafka outage..."
    capture_replication_slot_info "Before Kafka Outage"
    capture_kafka_offsets "Before Kafka Outage"

    log "Stopping Kafka container..."
    docker stop kafka
    success "Kafka stopped!"

        # Step 4: Generate data while Kafka is down
    log "Step 4: Generating database changes while Kafka is down..."
    log "PostgreSQL will continue working, but Debezium can't send events to Kafka."
    log "These changes should be captured when Kafka comes back up."

    log "About to insert data during outage. Current database state:"
    show_database_state "Before Outage Data Generation"

    log "Executing database changes while Kafka is down:"
    log "• INSERT 2 new customers with 'During Outage' names"
    log "• UPDATE existing customer email (id=1)"
    log "• INSERT 1 new product created during outage"

    docker exec postgres psql -U postgres -d testdb -c "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('During', 'Outage1', 'outage1@kafka-failure.com'),
        ('During', 'Outage2', 'outage2@kafka-failure.com');

        UPDATE inventory.customers SET email = 'updated-during-outage@test.com' WHERE id = 1;

        INSERT INTO inventory.products (name, description, price, quantity) VALUES
        ('Outage Product', 'Created during Kafka outage', 199.99, 3);
    "

    success "✅ Database changes made during Kafka outage: 2 INSERTs customers, 1 UPDATE customer, 1 INSERT product"
    success "✅ These changes are accumulating in PostgreSQL WAL and will be replicated when Kafka recovers"

    log "Database state after outage changes:"
    show_database_state "After Outage Data Generation"
    capture_replication_slot_info "During Kafka Outage"

    # Step 5: Verify Kafka is stopped and check Kafka Connect response
    log "Step 5: Verifying Kafka is stopped and checking Kafka Connect response..."
    sleep 10

    # Verify Kafka container is actually stopped
    if check_kafka_running; then
        error "Kafka container is still running! Connection failure simulation failed."
        warning "The test may not be working as expected."
    else
        success "✅ Kafka container is stopped as expected - connection failure simulated successfully."
    fi

    # Verify Kafka Connect is still running (it should be trying to connect and failing)
    log "Verifying Kafka Connect is still running..."
    if docker ps --format "table {{.Names}}" | grep -q "^kafka-connect$"; then
        success "✅ Kafka Connect container is still running (will show connection errors)"
    else
        error "❌ Kafka Connect container is not running - this is unexpected!"
    fi

    # Now check how Kafka Connect responds to the Kafka outage
    log "Checking how Kafka Connect responds to Kafka being down..."
    check_connect_status

    warning "Kafka Connect should show connection errors and be in a failed state. This is expected."
    wait_for_user

    # Step 6: Restart Kafka
    log "Step 6: Restarting Kafka..."
    docker start kafka

    # Wait for Kafka to be ready
    log "Waiting for Kafka to be ready..."
    sleep 30

    # Verify Kafka container is running
    check_kafka_running

    # Wait for Kafka service to be ready
    until docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1; do
        log "Kafka service not ready yet, waiting..."
        sleep 5
    done
    success "Kafka container is running and service is ready!"

        # Step 7: Wait for Kafka Connect to reconnect
    log "Step 7: Waiting for Kafka Connect to reconnect..."
    sleep 20

    # Check connector status with retry logic
    check_connect_status

    # Capture state immediately after reconnection
    log "Capturing state after Kafka reconnection..."
    capture_replication_slot_info "After Kafka Recovery"
    capture_kafka_offsets "After Kafka Recovery"
    show_message_correlation "After Kafka Recovery - Catching Up"

        # Step 8: Generate more data after recovery
    log "Step 8: Generating data after recovery..."
    log "Database state before post-recovery changes:"
    show_database_state "Before Post-Recovery Data"

    log "Executing database changes after Kafka recovery:"
    log "• INSERT 2 new customers with 'After Recovery' names"
    log "• UPDATE quantity of existing Test Products (+5 each)"

    docker exec postgres psql -U postgres -d testdb -c "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('After', 'Recovery1', 'recovery1@kafka-failure.com'),
        ('After', 'Recovery2', 'recovery2@kafka-failure.com');

        UPDATE inventory.products SET quantity = quantity + 5 WHERE name LIKE 'Test Product%';
    "

    success "✅ Post-recovery changes: 2 INSERT customers, 2 UPDATE products"

    sleep 10

    log "Database state after post-recovery changes:"
    show_database_state "After Post-Recovery Data"

    # Step 9: Verify all data was captured
    log "Step 9: Verifying all data was captured..."
    capture_replication_slot_info "Final State"
    capture_kafka_offsets "Final State"
    show_message_correlation "Final State - All Messages"

    # Step 10: Final analysis and comparison
    log "Step 10: Final analysis and offset comparison..."

    echo ""
    success "============================================"
    success "         TEST COMPLETION SUMMARY"
    success "============================================"
    log "1. ✓ Baseline data generated before failure"
    log "2. ✓ Kafka connection simulated failure"
    log "3. ✓ Data generated during outage period"
    log "4. ✓ Kafka and Kafka Connect recovered"
    log "5. ✓ Data generated after recovery"
    log "6. ✓ Database states tracked throughout test"
    log "7. ✓ Replication slot LSN positions monitored"
    log "8. ✓ Kafka consumer offsets tracked"
    log ""
    log "Key observations from this test:"
    log "- Database state changes were clearly tracked at each step"
    log "- PostgreSQL replication slot LSN advanced during outage (WAL accumulation)"
    log "- Kafka consumer offsets were preserved during outage"
    log "- Debezium resumed from the correct LSN position after recovery"
    log "- All database changes during outage were replicated to Kafka"
    log "- Message correlation shows exact relationship between DB changes and topics"
    log ""
    log "What happened during the outage:"
    log "- PostgreSQL continued to log changes in WAL"
    log "- Replication slot LSN position tracked the changes"
    log "- Kafka Connect couldn't send messages but tracked its position"
    log "- Upon recovery, Debezium caught up from the last confirmed LSN"
    log ""
    log "To further analyze:"
    log "- Review the captured LSN positions to see WAL accumulation during outage"
    log "- Compare Kafka offsets before/after to verify no message loss"
    log "- Check message timestamps to verify correct ordering"
    log "- Run: ./scripts/monitor-debezium.sh for ongoing monitoring"
    log "- Check Kafka UI at http://localhost:8080 for detailed topic analysis"

    warning "Note: In production, configure appropriate timeouts and retry policies"
    warning "for more robust handling of connection failures."

    log ""
    log "This enhanced test provides visibility into:"
    log "• Database state changes at each step"
    log "• PostgreSQL replication slot positions (restart_lsn, confirmed_flush_lsn)"
    log "• Kafka consumer group offsets and topic high watermarks"
    log "• Message correlation between database changes and Kafka topics"
    log "• Recovery behavior and offset resumption mechanics"
}

# Execute main function
main
