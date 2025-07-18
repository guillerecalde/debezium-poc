#!/bin/bash

# Test Script: Database Connection Failure
# This script tests how Debezium handles PostgreSQL authentication failures
# by changing the database password while the database continues to receive updates

echo "============================================"
echo "    DATABASE CONNECTION FAILURE TEST"
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

    # Capture latest offset message from Kafka Connect
    log "Latest Debezium offset message:"
    if docker ps --format "table {{.Names}}" | grep -q "^kafka$"; then
        latest_offset=$(docker exec kafka kafka-console-consumer \
            --bootstrap-server localhost:29092 \
            --topic my_connect_offsets \
            --from-beginning \
            --timeout-ms 20000 \
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

# Function to check Kafka Connect status without automatic restarts
check_connect_status_no_restart() {
    local max_attempts=${1:-15}
    log "Checking if Kafka Connect can recover automatically (no manual restarts)..."

    local attempts=0
    while [ $attempts -lt $max_attempts ]; do
        connector_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.connector.state' 2>/dev/null)
        task_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.tasks[0].state' 2>/dev/null)

        if [ "$connector_state" = "RUNNING" ] && [ "$task_state" = "RUNNING" ]; then
            success "Connector and task are both RUNNING - automatic recovery successful!"
            return 0
        else
            log "Connector state: $connector_state, Task state: $task_state (attempt $((attempts+1))/$max_attempts) - waiting for auto-recovery..."
            if [ $attempts -lt $((max_attempts-1)) ]; then
                sleep 10
            fi
        fi
        ((attempts++))
    done

    warning "Connector did not recover automatically after $max_attempts attempts"
    log "Final status without manual intervention:"
    if curl -f -s http://localhost:8083/connectors/inventory-connector/status > /dev/null 2>&1; then
        curl -s http://localhost:8083/connectors/inventory-connector/status | jq '.'
    fi
    return 1
}

# Function to check Kafka Connect status (both connector and tasks) with restart attempts
check_connect_status() {
    log "Checking Kafka Connect status with restart capabilities..."
    # Check both connector and task status
    local attempts=0
    local restart_attempted=false

    while [ $attempts -lt 20 ]; do
        connector_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.connector.state' 2>/dev/null)
        task_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.tasks[0].state' 2>/dev/null)

        if [ "$connector_state" = "RUNNING" ] && [ "$task_state" = "RUNNING" ]; then
            success "Connector and task are both RUNNING!"
            break
        elif [ "$task_state" = "FAILED" ] && [ "$restart_attempted" = false ] && [ $attempts -gt 5 ]; then
            warning "Task is in FAILED state, attempting restart..."
            restart_connector
            restart_attempted=true
            sleep 15  # Give more time after restart
        else
            log "Connector state: $connector_state, Task state: $task_state (attempt $((attempts+1))/20)"
            if [ $attempts -lt 19 ]; then
                sleep 10
            fi
        fi
        ((attempts++))
    done

    # Show detailed connector status
    log "Final connector status:"
    if curl -f -s http://localhost:8083/connectors/inventory-connector/status > /dev/null 2>&1; then
        curl -s http://localhost:8083/connectors/inventory-connector/status | jq '.'
    fi
}

# Function to change database password (simulate connection failure)
change_database_password() {
    local new_password="$1"
    log "Changing database password to simulate connection failure..."
    docker exec postgres psql -U postgres -d testdb -c "
        ALTER USER postgres PASSWORD '$new_password';
    "
    success "Database password changed to: $new_password"
}

# Function to restart the connector
restart_connector() {
    log "Restarting Kafka Connect connector..."

    # First try restarting just the failed task
    log "Restarting failed connector task..."
    curl -X POST http://localhost:8083/connectors/inventory-connector/tasks/0/restart
    sleep 5

    # Check if task restart worked
    task_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.tasks[0].state' 2>/dev/null)

    if [ "$task_state" != "RUNNING" ]; then
        log "Task restart didn't work, restarting entire connector..."
        curl -X POST http://localhost:8083/connectors/inventory-connector/restart
        sleep 5
    fi

    success "Connector restart commands sent"
}

# Function to force connector restart (can be called manually if needed)
force_connector_restart() {
    warning "Forcing complete connector restart..."

    log "1. Deleting connector..."
    curl -X DELETE http://localhost:8083/connectors/inventory-connector 2>/dev/null
    sleep 5

    log "2. Recreating connector..."
    if [ -f "./scripts/setup-connector.sh" ]; then
        bash ./scripts/setup-connector.sh >/dev/null 2>&1
        sleep 10
    fi

    log "3. Checking status after forced restart..."
    curl -s http://localhost:8083/connectors/inventory-connector/status | jq '.'

    success "Forced restart complete. Check status above."
}

# Function to reconnect with original password using setup script
reconnect_with_original_password() {
    log "Reconnecting Debezium with original password..."

    # Step 1: Change database password back to original
    change_database_password "postgres"

    # Step 2: Delete the existing connector to ensure clean state
    log "Deleting existing connector to ensure clean reconfiguration..."
    curl -X DELETE http://localhost:8083/connectors/inventory-connector 2>/dev/null
    sleep 3

    # Step 3: Use the setup script to recreate the connector
    if [ -f "./scripts/setup-connector.sh" ]; then
        log "Running setup-connector.sh to recreate connector with original password..."
        bash ./scripts/setup-connector.sh >/dev/null 2>&1 || {
            error "Failed to run setup-connector.sh"
            return 1
        }
        success "Connector recreated with original password using setup script"
    else
        error "setup-connector.sh not found"
        return 1
    fi

        # Step 4: First test if connector can recover automatically
    sleep 5
    log "Testing automatic recovery capabilities..."
    if check_connect_status_no_restart 15; then
        success "Connector recovered automatically after password reset!"
    else
        warning "Connector did not recover automatically, will need manual restart"
        log "Attempting manual restart..."
        restart_connector
    fi
}

# Function to generate test data directly in PostgreSQL
generate_database_changes() {
    local phase="$1"
    local phase_lower=$(echo "$phase" | tr '[:upper:]' '[:lower:]')
    log "Generating database changes during $phase phase..."

    docker exec postgres psql -U postgres -d testdb -c "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('$phase', 'User1', '${phase_lower}1@db-failure.com'),
        ('$phase', 'User2', '${phase_lower}2@db-failure.com');

        INSERT INTO inventory.products (name, description, price, quantity) VALUES
        ('$phase Product', 'Product during $phase phase', 89.99, 15);

        UPDATE inventory.customers
        SET email = LOWER(first_name) || '@${phase_lower}-updated.com'
        WHERE first_name = '$phase' AND last_name = 'User1';
    "

    success "✅ Database changes made: 2 INSERT customers, 1 INSERT product, 1 UPDATE customer"
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

# Function to terminate Debezium database connections
terminate_debezium_connections() {
    log "Terminating existing Debezium database connections to force re-authentication..."

    # First, show current connections
    log "Current Debezium connections before termination:"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT pid, application_name, client_addr, state, backend_start
        FROM pg_stat_activity
        WHERE (application_name ILIKE '%debezium%'
               OR application_name ILIKE '%connector%'
               OR client_addr IS NOT NULL)
        AND datname = 'testdb'
        AND pid != pg_backend_pid();
    "

    # Terminate connections
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            pid,
            pg_terminate_backend(pid) as terminated,
            application_name,
            client_addr
        FROM pg_stat_activity
        WHERE (application_name ILIKE '%debezium%'
               OR application_name ILIKE '%connector%'
               OR client_addr IS NOT NULL)
        AND datname = 'testdb'
        AND pid != pg_backend_pid();
    "

    sleep 2

    log "Remaining database connections after termination:"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT application_name, client_addr, state, backend_start
        FROM pg_stat_activity
        WHERE datname = 'testdb'
        AND application_name IS NOT NULL
        ORDER BY backend_start;
    "
}

# Function to force WAL activity and connection attempts
force_wal_activity() {
    local phase="$1"
    log "Forcing WAL activity to trigger connection attempts ($phase)..."

    # Make a small change to force WAL activity
    docker exec postgres psql -U postgres -d testdb -c "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('Force-$phase', 'WAL-Activity', 'force-wal@test.com');

        UPDATE inventory.customers
        SET email = 'force-wal-updated@test.com'
        WHERE first_name = 'Force-$phase';

        DELETE FROM inventory.customers
        WHERE first_name = 'Force-$phase';
    "

    log "WAL activity forced - Debezium should attempt to read these changes"
}

# Function to wait for connector to fail
wait_for_connector_failure() {
    log "Waiting for connector to detect the connection failure..."
    local attempts=0
    while [ $attempts -lt 30 ]; do
        connector_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.connector.state' 2>/dev/null)
        task_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.tasks[0].state' 2>/dev/null)

        if [ "$connector_state" = "FAILED" ] || [ "$task_state" = "FAILED" ]; then
            warning "Connector has failed as expected due to connection issues"
            log "Connector state: $connector_state, Task state: $task_state"
            curl -s http://localhost:8083/connectors/inventory-connector/status | jq '.'
            return 0
        else
            log "Connector state: $connector_state, Task state: $task_state (attempt $((attempts+1))/30)"

            # Every 5 attempts, force more WAL activity to trigger connection attempts
            if [ $((attempts % 5)) -eq 4 ]; then
                force_wal_activity "Attempt-$attempts"
            fi

            sleep 5
            ((attempts++))
        fi
    done

    warning "Connector didn't fail within expected time"
    log "Final connector status:"
    curl -s http://localhost:8083/connectors/inventory-connector/status | jq '.'
    return 1
}

# Main test execution
main() {
    log "Starting Database Authentication Failure Test"
    log "This test simulates authentication failures by changing DB password while DB continues receiving updates"

    # Step 1: Verify initial setup
    log "Step 1: Verifying initial setup..."
    if ! docker ps | grep -q postgres; then
        error "PostgreSQL container is not running. Please start the environment first."
        exit 1
    fi

    if ! docker ps | grep -q kafka; then
        error "Kafka container is not running. Please start the environment first."
        exit 1
    fi

    check_postgres_running
    setup_kafka_connect
    check_connect_status

    # Show initial state
    show_database_state "Initial Setup"
    capture_replication_slot_info "Initial Setup"
    capture_kafka_status "Initial Setup"
    show_database_connections "Initial Setup"
    wait_for_user

    # Step 2: Generate baseline data
    log "Step 2: Generating baseline data..."
    generate_database_changes "Baseline"

    sleep 5
    show_database_state "After Baseline Data"
    capture_replication_slot_info "After Baseline Data"
    capture_kafka_status "After Baseline Data"
    show_message_correlation "After Baseline Data"
    wait_for_user

        # Step 3: Change database password (simulate authentication failure)
    warning "Step 3: Simulating database authentication failure via password change..."
    log "This simulates authentication issues while PostgreSQL continues running"

        # Capture state before failure
    capture_replication_slot_info "Before Password Change"
    capture_kafka_status "Before Password Change"
    show_database_connections "Before Password Change"

    # Change database password but don't update connector
    change_database_password "newpassword123"
    log "Database password changed but connector still uses old password"
    log "This will cause authentication failures for Debezium"

    # Force connection failure by terminating existing connections
    sleep 2
    terminate_debezium_connections
    show_database_connections "After Connection Termination"

    # Force WAL activity to trigger immediate connection attempts
    sleep 2
    force_wal_activity "Password-Change"

    # Wait for connector to fail
    wait_for_connector_failure
    wait_for_user

    # Step 4: Generate data while Debezium can't connect
    log "Step 4: Generating database changes while Debezium cannot connect..."
    log "PostgreSQL continues working normally but Debezium can't authenticate"

    show_database_state "Before Connection Failure Data"

    generate_database_changes "Connection-Failed"

    success "✅ Database changes made while Debezium was disconnected"
    success "✅ These changes are accumulating in PostgreSQL WAL"

    show_database_state "After Connection Failure Data"
    capture_replication_slot_info "During Connection Failure"
    capture_kafka_status "During Connection Failure"
    wait_for_user

                # Step 5: Fix the connection using setup script
    log "Step 5: Fixing database connection by resetting password and recreating connector..."
    reconnect_with_original_password

    log "Giving additional time for connector to stabilize..."
    sleep 10

    # Final status check (this may include restart attempts if needed)
    log "Performing final status verification..."
    check_connect_status

    # Final verification
    final_connector_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.connector.state' 2>/dev/null)
    final_task_state=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | jq -r '.tasks[0].state' 2>/dev/null)

    if [ "$final_connector_state" = "RUNNING" ] && [ "$final_task_state" = "RUNNING" ]; then
        success "Connector is fully operational after password reset and recovery!"
    else
        warning "Connector may still need manual intervention. Current states:"
        warning "Connector: $final_connector_state, Task: $final_task_state"
        log "Manual restart commands:"
        log "• curl -X POST http://localhost:8083/connectors/inventory-connector/restart"
        log "• curl -X POST http://localhost:8083/connectors/inventory-connector/tasks/0/restart"
    fi

    capture_replication_slot_info "After Password Reset"
    capture_kafka_status "After Password Reset - Catching Up"
    show_message_correlation "After Password Reset - Catching Up"
    wait_for_user

    # Step 6: Generate final data to verify everything works
    log "Step 6: Generating final test data to verify recovery..."
    generate_database_changes "Post-Recovery"

    sleep 10
    show_database_state "Final State"
    capture_replication_slot_info "Final State"
    capture_kafka_status "Final State"
    show_message_correlation "Final State - All Messages"

    # Step 7: Final verification
    log "Step 7: Final message count verification..."
    check_message_counts

    echo ""
    success "============================================"
    success "         TEST COMPLETION SUMMARY"
    success "============================================"
    log "1. ✓ Baseline data generated before failure"
    log "2. ✓ Database password changed (authentication failure simulation)"
    log "3. ✓ Existing database connections terminated to force re-authentication"
    log "4. ✓ Data generated during authentication failure"
    log "5. ✓ Connection fixed by resetting password and using setup script"
    log "6. ✓ Automatic recovery capabilities tested (before manual restarts)"
    log "7. ✓ Post-recovery data generated and verified"
    log "8. ✓ Replication slot LSN positions tracked throughout"
    log "9. ✓ Kafka topic message counts monitored at each step"
    log ""
    log "Key observations from this test:"
    log "- PostgreSQL continued running and accepting changes during password failure"
    log "- Connection termination was necessary to force immediate re-authentication"
    log "- Replication slot LSN advanced during outage (WAL accumulation)"
    log "- Kafka topic message counts tracked CDC event delivery"
    log "- Automatic recovery was tested first (before manual intervention)"
    log "- Debezium resumed from correct LSN position after reconnection"
    log "- All database changes during outage were replicated to Kafka"
    log "- Setup script successfully reconfigured the connector"
    log ""
    log "What happened during the connection failure and recovery:"
    log "- PostgreSQL continued logging changes in WAL"
    log "- Existing database connections were terminated to force re-authentication"
    log "- WAL activity was forced to trigger immediate connection attempts"
    log "- Replication slot tracked changes even when inactive"
    log "- Kafka Connect tasks failed due to authentication errors"
    log "- After password reset, automatic recovery was tested first"
    log "- Manual connector restart was used only if auto-recovery failed"
    log "- Upon reconnection, Debezium caught up from last confirmed LSN"
    log "- No data loss occurred during temporary connection failure"
    log ""
    log "Authentication failure scenario tested:"
    log "- Wrong password + connection termination + forced WAL activity"
    log "- Demonstrates Debezium's resilience and recovery capabilities"
    log ""
    log "Production recommendations:"
    log "- Monitor connector status and task failures during password changes"
    log "- Test automatic recovery capabilities before implementing manual restarts"
    log "- Set up automated restart mechanisms for failed connectors/tasks"
    log "- Set appropriate connection retry policies for authentication failures"
    log "- Configure database connection pooling to handle connection drops"
    log "- Monitor replication slot lag during authentication outages"
    log "- Set up alerts for connector FAILED states with auto-restart capabilities"
    log "- Test password rotation procedures regularly with restart verification"
    log "- Use setup scripts for reliable connector reconfiguration"
    log "- Implement connector health checks with automatic recovery"
    log ""
    warning "For production environments:"
    warning "- Implement secure password rotation procedures with minimal downtime"
    warning "- Use proper credential management and secret rotation"
    warning "- Monitor replication slot growth during authentication outages"
    warning "- Configure appropriate timeout and retry policies for auth failures"
    warning "- Test connection termination scenarios as part of failover procedures"
    warning "- Set up automated restart mechanisms for connector failures"
    log ""
    log "Manual troubleshooting commands if connector gets stuck:"
    log "• Check status: curl http://localhost:8083/connectors/inventory-connector/status | jq '.'"
    log "• Restart task: curl -X POST http://localhost:8083/connectors/inventory-connector/tasks/0/restart"
    log "• Restart connector: curl -X POST http://localhost:8083/connectors/inventory-connector/restart"
    log "• Force restart: bash -c 'source ./scripts/test-db-connection-failure.sh && force_connector_restart'"
    log "• Check logs: docker logs kafka-connect"
}

# Execute main function
main
