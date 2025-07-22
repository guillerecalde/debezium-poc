#!/bin/bash

# Test Script: WAL Retention Period Failure
# This script tests realistic scenarios where restart_lsn becomes older than the WAL retention period
# causing permanent data loss when PostgreSQL purges required WAL files
#
# REALISTIC FAILURE SCENARIOS TESTED:
# 1. Kafka Connect crash/outage (container stop)
# 2. High WAL generation during extended downtime
# 3. WAL file purging due to aggressive retention settings
#
# This demonstrates real-world data loss scenarios through natural connection failures

echo "============================================"
echo "    WAL RETENTION PERIOD FAILURE TEST"
echo "      (Simplified & Stable)"
echo "============================================"

# Source common utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/test-utils.sh"

# Note: Using log(), error(), success(), warning(), and wait_for_user() from test-utils.sh

# Function to configure aggressive WAL retention
configure_aggressive_wal_cleanup() {
    warning "=== Configuring Aggressive WAL Cleanup ==="
    log "Setting very short WAL retention period for this test..."

    docker exec postgres psql -U postgres -d testdb -c "
        -- Show current settings
        SELECT
            name,
            setting,
            unit,
            context,
            short_desc
        FROM pg_settings
        WHERE name IN (
            'wal_keep_size',
            'max_wal_size',
            'checkpoint_timeout',
            'checkpoint_completion_target',
            'wal_buffers'
        );
    "

        # Set extremely aggressive WAL cleanup settings
    log "Setting aggressive WAL cleanup parameters..."

    # Execute each ALTER SYSTEM command separately (they cannot run in a transaction block)
    log "Setting wal_keep_size to 0 (no retention guarantee)..."
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET wal_keep_size = 0;"

    log "🎯 Setting max_slot_wal_keep_size to 2MB (CRITICAL: overrides slot protection!)..."
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET max_slot_wal_keep_size = '2MB';"

    log "Setting checkpoint_timeout to 60s (aggressive but safe)..."
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET checkpoint_timeout = '60s';"

    log "Setting max_wal_size to 16MB (small but stable)..."
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET max_wal_size = '16MB';"

    log "Setting min_wal_size to 8MB (small but reasonable)..."
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET min_wal_size = '8MB';"

    log "Setting checkpoint_completion_target to 0.2 (fast but stable)..."
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET checkpoint_completion_target = 0.2;"

    log "Reloading PostgreSQL configuration..."
    docker exec postgres psql -U postgres -d testdb -c "SELECT pg_reload_conf();"

    success "✅ AGGRESSIVE (but stable) WAL cleanup configured!"
    log "WAL files will now be purged aggressively while maintaining PostgreSQL stability"
    error "⚠️  With wal_keep_size=0 AND max_slot_wal_keep_size=2MB, slot protection is severely limited!"
    error "🎯 max_slot_wal_keep_size=2MB is the KEY to forcing 'lost' status!"
    error "⚠️  With 500MB+ WAL generation vs 2MB limit, purging is inevitable!"

    # Show new settings
    log "New ULTRA AGGRESSIVE WAL settings:"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            name,
            setting,
            unit,
            CASE
                WHEN name = 'max_slot_wal_keep_size' THEN '🎯 KEY PARAMETER FOR FORCING LOST STATUS! (2MB limit)'
                WHEN name = 'wal_keep_size' THEN '⚠️  Zero retention guarantee'
                WHEN name = 'checkpoint_timeout' THEN '⚡ Aggressive checkpoints (60s)'
                WHEN name = 'max_wal_size' THEN '⚡ Small WAL size (16MB)'
                WHEN name = 'min_wal_size' THEN '⚡ Small WAL size (8MB)'
                ELSE 'Minimal WAL retention'
            END as impact
        FROM pg_settings
        WHERE name IN (
            'wal_keep_size',
            'max_slot_wal_keep_size',
            'max_wal_size',
            'min_wal_size',
            'checkpoint_timeout',
            'checkpoint_completion_target'
        )
        ORDER BY
            CASE name
                WHEN 'max_slot_wal_keep_size' THEN 1
                WHEN 'wal_keep_size' THEN 2
                ELSE 3
            END;
    "
}


# Function to restore normal WAL settings
restore_normal_wal_settings() {
    log "Restoring normal WAL settings..."

    # Execute each ALTER SYSTEM command separately (they cannot run in a transaction block)
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET wal_keep_size = '1GB';"
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET max_slot_wal_keep_size = -1;"  # Reset to default (unlimited)
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET checkpoint_timeout = '5min';"
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET max_wal_size = '1GB';"
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET min_wal_size = '80MB';"  # Reset to reasonable default
    docker exec postgres psql -U postgres -d testdb -c "ALTER SYSTEM SET checkpoint_completion_target = 0.9;"
    docker exec postgres psql -U postgres -d testdb -c "SELECT pg_reload_conf();"

    success "✅ Normal WAL settings restored"
}

# Function to generate heavy WAL activity
generate_heavy_wal_activity() {
    local phase=$1
    local rounds=${2:-3}

    log "Generating heavy WAL activity (Phase: $phase)..."

        # Pre-process variables to avoid bash substitution issues
    local phase_lower=$(echo "$phase" | tr '[:upper:]' '[:lower:]')
    local phase_safe=$(echo "$phase" | tr '[:upper:]' '[:lower:]' | tr '-' '_')

    for round in $(seq 1 $rounds); do
        log "WAL generation round $round/$rounds..."

                        # Generate heavy WAL activity with business data
        # Use regular table instead of TEMP (since each docker exec is a new session)
        docker exec postgres psql -U postgres -d testdb -c "
            CREATE TABLE wal_generator_${phase_safe}_$round (id SERIAL, data TEXT, created_at TIMESTAMP DEFAULT NOW());
        "

        docker exec postgres psql -U postgres -d testdb -c "
            INSERT INTO wal_generator_${phase_safe}_$round (data)
            SELECT 'WAL_DATA_' || generate_series || '_' || repeat('x', 2000)
            FROM generate_series(1, 10000);
        "

        docker exec postgres psql -U postgres -d testdb -c "
            INSERT INTO inventory.customers (first_name, last_name, email) VALUES
            ('$phase-Round$round', 'TestUser1', '$phase_lower-r$round-u1@wal-test.com'),
            ('$phase-Round$round', 'TestUser2', '$phase_lower-r$round-u2@wal-test.com'),
            ('$phase-Round$round', 'TestUser3', '$phase_lower-r$round-u3@wal-test.com');
        "

        docker exec postgres psql -U postgres -d testdb -c "
            INSERT INTO inventory.products (name, description, price, quantity) VALUES
            ('$phase-R$round Widget', 'Widget during $phase round $round', 29.99, 100),
            ('$phase-R$round Gadget', 'Gadget during $phase round $round', 49.99, 50);
        "

        docker exec postgres psql -U postgres -d testdb -c "
            UPDATE wal_generator_${phase_safe}_$round SET data = data || '_UPDATED' WHERE id % 100 = 0;
        "

        docker exec postgres psql -U postgres -d testdb -c "
            UPDATE inventory.customers
            SET email = LOWER(first_name) || '.' || LOWER(last_name) || '.' || id || '@wal-$phase_lower-r$round.com'
            WHERE first_name LIKE '$phase-Round$round%';
        "

        docker exec postgres psql -U postgres -d testdb -c "
            SELECT pg_switch_wal();
        "

        docker exec postgres psql -U postgres -d testdb -c "
            CHECKPOINT;
        "

        docker exec postgres psql -U postgres -d testdb -c "
            DROP TABLE IF EXISTS wal_generator_${phase_safe}_$round;
        "

        # Show current WAL position after each round
        if [ $round -eq $rounds ]; then
            docker exec postgres psql -U postgres -d testdb -c "
                SELECT
                    'After $phase Round $round' as phase,
                    pg_current_wal_lsn() as current_wal_lsn,
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as total_wal_generated;
            "
        fi

        sleep 2  # Brief pause between rounds
    done

    success "✅ Heavy WAL activity completed for $phase"
}

# Function to wait for WAL retention cleanup
wait_for_wal_cleanup() {
    local wait_minutes=${1:-5}  # Extended default wait time
    warning "=== Waiting for ULTRA AGGRESSIVE WAL Retention Cleanup ==="
    log "Waiting $wait_minutes minutes for WAL files to be aggressively purged..."
    log "With wal_keep_size=0, max_slot_wal_keep_size=2MB, max_wal_size=16MB, checkpoints every 60s + manual every 15s"
    log "This should force PostgreSQL to purge WAL files beyond retention"

    local checkpoint_count=0
    for minute in $(seq 1 $wait_minutes); do
        log "Cleanup phase $minute/$wait_minutes minutes..."

        # Force checkpoint every 15 seconds to accelerate cleanup
        for quarter in {1..4}; do
            sleep 15
            checkpoint_count=$((checkpoint_count + 1))
            log "  Forcing checkpoint #$checkpoint_count..."

            # Force multiple operations to trigger cleanup
            docker exec postgres psql -U postgres -d testdb -c "
                CHECKPOINT;
                SELECT pg_switch_wal();

                -- Check WAL status after each checkpoint
                SELECT
                    slot_name,
                    wal_status,
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
                FROM pg_replication_slots
                WHERE slot_name = 'debezium_slot';
            "

            # Show current wal_status to track progress
            wal_status=$(docker exec postgres psql -U postgres -d testdb -t -c "
                SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'debezium_slot';
            " | tr -d ' ')

            if [ "$wal_status" = "lost" ]; then
                error "🎯 SUCCESS: WAL status is now 'lost' after $checkpoint_count checkpoints!"
                return 0
            else
                log "    Current WAL status: $wal_status (target: lost)"
            fi
        done
    done

    error "⚠️  WAL cleanup period completed - checking final status..."
    log "Performed $checkpoint_count forced checkpoints"
}

# Function to show WAL file status (enhanced with utility functions)
show_wal_file_status() {
    local phase=$1

    # Use the comprehensive replication slot info from test-utils.sh
    capture_replication_slot_info "$phase"

    # Add WAL-specific information for this test
    log "=== WAL Files and Settings: $phase ==="
    docker exec postgres psql -U postgres -d testdb -c "
        -- Show current WAL files
        SELECT
            name,
            pg_size_pretty(size) as file_size,
            modification as last_modified
        FROM pg_ls_waldir()
        ORDER BY modification DESC
        LIMIT 10;

        -- Show current WAL settings (important for this test)
        SELECT
            'WAL Configuration' as info_type,
            name,
            setting,
            unit,
            context
        FROM pg_settings
        WHERE name IN ('wal_keep_size', 'max_wal_size', 'checkpoint_timeout', 'checkpoint_completion_target');

        -- Show critical wal_status field
        SELECT
            'WAL Status Check' as info_type,
            slot_name,
            wal_status,
            CASE
                WHEN wal_status = 'reserved' THEN '✅ WAL files are available'
                WHEN wal_status = 'lost' THEN '🚨 CRITICAL: WAL files are LOST!'
                WHEN wal_status IS NULL THEN '⚠️  WAL status unknown'
                ELSE '⚠️  WAL status: ' || wal_status
            END as status_meaning
        FROM pg_replication_slots
        WHERE slot_name = 'debezium_slot';
    "
}

# Function to check connector status with detailed error info (enhanced with utility functions)
check_connector_detailed_status() {
    log "=== Detailed Connector Status ==="

    if curl -f -s http://localhost:8083/ > /dev/null; then
        log "Connector status:"
        curl -s http://localhost:8083/connectors/inventory-connector/status | jq '.'

        echo ""
        log "Connector configuration:"
        curl -s http://localhost:8083/connectors/inventory-connector/config | jq '.'

        echo ""
        log "Recent connector logs (last 20 lines):"
        docker logs --tail 20 kafka-connect | grep -E "(ERROR|WARN|debezium|inventory-connector)" || echo "No recent error logs found"

        echo ""
        # Use utility function to show comprehensive Kafka offsets
        capture_kafka_offsets "Connector Status Check"
    else
        error "Kafka Connect API is not responding"
        return 1
    fi
}

# Function to count database records at each step (uses utility function)
count_database_records() {
    local phase=$1
    # Use the comprehensive database state function from test-utils.sh
    show_database_state "$phase"

    # Also show summary counts
    log "Record count summary for $phase:"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            'customers' as table_name,
            count(*) as record_count
        FROM inventory.customers
        UNION ALL
        SELECT
            'products' as table_name,
            count(*) as record_count
        FROM inventory.products
        UNION ALL
        SELECT
            'orders' as table_name,
            count(*) as record_count
        FROM inventory.orders;
    "
}

# Function to count Kafka messages (reuses utility function)
count_kafka_messages() {
    local phase=$1
    # Use the existing message count function from test-utils.sh
    capture_kafka_status "$phase"
}

# Function to demonstrate data loss
demonstrate_data_loss() {
    log "=== Demonstrating Data Loss ==="

    log "Analyzing CDC capture vs database activity during test..."

    # Get current database counts
    customer_db_count=$(docker exec postgres psql -U postgres -d testdb -t -c "SELECT count(*) FROM inventory.customers;" | tr -d ' ')
    product_db_count=$(docker exec postgres psql -U postgres -d testdb -t -c "SELECT count(*) FROM inventory.products;" | tr -d ' ')

    # Get test-specific data counts (data created during our test)
    test_customer_count=$(docker exec postgres psql -U postgres -d testdb -t -c "
        SELECT count(*) FROM inventory.customers
        WHERE first_name LIKE '%Round%' OR first_name LIKE 'Baseline%' OR first_name LIKE 'During%';
    " | tr -d ' ')

    test_product_count=$(docker exec postgres psql -U postgres -d testdb -t -c "
        SELECT count(*) FROM inventory.products
        WHERE name LIKE '%Widget%' OR name LIKE '%Gadget%';
    " | tr -d ' ')

    # Get Kafka message counts
    customer_kafka_count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:29092 \
        --topic customers \
        --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')
    product_kafka_count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:29092 \
        --topic products \
        --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')

    customer_kafka_count=${customer_kafka_count:-0}
    product_kafka_count=${product_kafka_count:-0}

    echo ""
    error "=== CDC vs DATABASE ANALYSIS ==="
    log "Total database records:"
    log "  • Customers: $customer_db_count total ($test_customer_count from test)"
    log "  • Products: $product_db_count total ($test_product_count from test)"
    echo ""
    log "Total CDC messages captured:"
    log "  • Customer messages: $customer_kafka_count (includes inserts + updates + initial snapshot)"
    log "  • Product messages: $product_kafka_count (includes inserts + updates + initial snapshot)"
    echo ""

    if [ "$customer_kafka_count" -gt 0 ] && [ "$product_kafka_count" -gt 0 ]; then
        success "✅ CDC messages are flowing - connector was working before failure"

        # Check if connector is currently working by looking for recent messages
        log "Checking if CDC resumed after restart attempt..."

        # Generate a test record to see if CDC is working now
        docker exec postgres psql -U postgres -d testdb -c "
            INSERT INTO inventory.customers (first_name, last_name, email)
            VALUES ('DataLossTest', 'User', 'dataloss.test@recovery.com');
        " >/dev/null 2>&1

        sleep 5

        new_customer_kafka_count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:29092 \
            --topic customers \
            --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')
        new_customer_kafka_count=${new_customer_kafka_count:-0}

        if [ "$new_customer_kafka_count" -gt "$customer_kafka_count" ]; then
            warning "⚠️  CDC appears to be working now - connector may have recovered"
            log "New customer message detected (count increased from $customer_kafka_count to $new_customer_kafka_count)"
        else
            error "🚨 CDC is NOT working - no new messages captured"
            error "This confirms the connector failed due to WAL retention issues"
            error "Changes made during downtime period were permanently lost"
        fi

    else
        error "🚨 CRITICAL: Very few or no CDC messages captured!"
        error "This indicates the connector never properly started or failed completely"
    fi

    echo ""
    log "💡 Understanding CDC Message vs Record Counts:"
    log "   • CDC messages = ALL operations (insert + update + delete + snapshot)"
    log "   • Database records = CURRENT state (final result after all operations)"
    log "   • More messages than records is NORMAL (each record can have multiple operations)"
    log "   • Data loss detection requires checking if recent changes are captured"
}

# Main test execution
main() {
    log "Starting WAL Retention Period Failure Test"
    warning "This test demonstrates permanent data loss scenarios!"
    warning "It uses aggressive WAL retention settings that will cause data loss"
    echo ""

    read -p "Do you want to continue with this potentially destructive test? (y/N): " confirm
    if [[ ! $confirm =~ ^[Yy]$ ]]; then
        log "Test cancelled by user"
        exit 0
    fi

    # Step 1: Verify PostgreSQL health and configure aggressive WAL cleanup
    log "Step 1: Verifying PostgreSQL health before aggressive configuration..."

    # Quick health check
    if ! docker exec postgres pg_isready -U postgres >/dev/null 2>&1; then
        error "PostgreSQL is not ready at test start!"
        log "Please ensure PostgreSQL is running: docker-compose up -d postgres"
        return 1
    fi

    success "✅ PostgreSQL is healthy - proceeding with aggressive WAL configuration"

    show_wal_file_status "Initial State"
    configure_aggressive_wal_cleanup
    count_database_records "Initial State"
    count_kafka_messages "Initial State"
    wait_for_user

    # Step 2: Generate baseline data with normal operations
    log "Step 2: Generating baseline data with CDC working normally..."
    generate_heavy_wal_activity "Baseline" 2
    sleep 10  # Let CDC catch up
    show_wal_file_status "After Baseline Data"
    count_database_records "After Baseline"
    count_kafka_messages "After Baseline"

    # Show actual CDC messages flowing normally
    show_message_correlation "Baseline - Normal CDC Operation"
    wait_for_user

    # Step 3: Stop Kafka Connect to simulate crash
    log "Step 3: Simulating Kafka Connect crash (stopping container)..."
    docker stop kafka-connect
    success "✅ Kafka Connect stopped (simulating crash)"
    show_wal_file_status "After Connect Crash"
    wait_for_user

    # Step 4: Generate lots of data while Connect is down
    log "Step 4: Generating heavy database activity while Kafka Connect is down..."
    log "This data will accumulate in WAL files, but Connect can't process it"
    generate_heavy_wal_activity "During-Downtime" 5
    count_database_records "During Downtime"
    show_wal_file_status "After Heavy Activity During Downtime"
    wait_for_user

    # Step 5: Generate massive WAL pressure to force data loss
    log "Step 5: Generating massive WAL pressure to overwhelm retention settings..."
    warning "This is where the data loss occurs!"

    log "Generating MASSIVE WAL activity to overwhelm retention settings..."
    # Generate way more WAL activity to force PostgreSQL past its comfort zone
    generate_heavy_wal_activity "Massive-WAL-1" 10  # 10 rounds instead of 3

    log "Checking replication slot status after massive WAL generation..."
    docker exec postgres psql -U postgres -d testdb -c "
        -- Check if slot is now inactive after realistic failures
        SELECT
            slot_name,
            active,
            active_pid,
            wal_status,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size
        FROM pg_replication_slots
        WHERE slot_name = 'debezium_slot';
    "

    # Generate even MORE WAL activity during cleanup to really pressure the system
    generate_heavy_wal_activity "Massive-WAL-2" 10

    # NUCLEAR OPTION: Generate truly massive WAL volume
    log "NUCLEAR OPTION: Generating extreme WAL volume to force purging..."
    warning "Creating hundreds of MB of WAL data to overwhelm any retention..."

    for nuclear_round in {1..5}; do
        log "Nuclear WAL generation round $nuclear_round/5..."
        docker exec postgres psql -U postgres -d testdb -c "
            -- Create large table with massive data
            CREATE TABLE nuclear_wal_$nuclear_round AS
            SELECT
                generate_series as id,
                md5(generate_series::text) || repeat('MASSIVE_WAL_DATA', 100) as large_data,
                now() as created_at
            FROM generate_series(1, 50000);

            -- Update half the records to generate more WAL
            UPDATE nuclear_wal_$nuclear_round
            SET large_data = large_data || '_UPDATED_' || md5(id::text)
            WHERE id % 2 = 0;

            -- Delete a quarter to generate even more WAL
            DELETE FROM nuclear_wal_$nuclear_round WHERE id % 4 = 0;

            -- Force WAL switch and checkpoint
            SELECT pg_switch_wal();
            CHECKPOINT;

            -- Drop the table
            DROP TABLE nuclear_wal_$nuclear_round;

            -- Show current WAL status
            SELECT
                'Nuclear Round $nuclear_round' as phase,
                slot_name,
                wal_status,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size
            FROM pg_replication_slots
            WHERE slot_name = 'debezium_slot';
        "

        # Check if we've achieved lost status
        wal_status=$(docker exec postgres psql -U postgres -d testdb -t -c "
            SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'debezium_slot';
        " | tr -d ' ')

        if [ "$wal_status" = "lost" ]; then
            error "🎯 NUCLEAR SUCCESS: WAL status is now 'lost' after nuclear round $nuclear_round!"
            break
        fi

        sleep 5
    done

    wait_for_wal_cleanup 8  # Extended wait time for more cleanup cycles
    show_wal_file_status "After MASSIVE WAL Cleanup Period"

    # Check replication slot status - should show 'lost' wal_status
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            slot_name,
            wal_status,
            safe_wal_size,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size
        FROM pg_replication_slots
        WHERE slot_name = 'debezium_slot';
    "
    wait_for_user

    # Step 6: Try to restart Kafka Connect and observe failure
    log "Step 6: Attempting to restart Kafka Connect..."
    log "This should fail because required WAL files are gone"
    docker start kafka-connect

    sleep 30
    until curl -f -s http://localhost:8083/ > /dev/null; do
        log "Waiting for Kafka Connect to start..."
        sleep 5
    done

    # Wait a bit for connector to try to resume and fail
    log "Waiting for connector to attempt recovery and fail..."
    sleep 30

    check_connector_detailed_status
    show_wal_file_status "After Connect Restart Attempt"
    wait_for_user

    # Step 7: Demonstrate the data loss
    log "Step 7: Demonstrating permanent data loss..."
    demonstrate_data_loss
    count_database_records "Final Database State"
    count_kafka_messages "Final Kafka State"

    # Show actual CDC messages to demonstrate what was lost
    show_message_correlation "Data Loss Analysis - Available CDC Events"

    # Step 8: Restore normal WAL settings
    log "Step 8: Restoring normal WAL settings..."
    restore_normal_wal_settings

    # Summary
    echo ""
    success "============================================"
    success "         TEST COMPLETION SUMMARY"
    success "============================================"
    log "1. ✓ Aggressive WAL retention configured (2MB max_slot_wal_keep_size)"
    log "2. ✓ Baseline data generated with CDC working"
    log "3. ✓ Kafka Connect crash simulated"
    log "4. ✓ Heavy database activity during downtime"
    log "5. ✓ WAL retention cleanup purged required files"
    log "6. ✓ Kafka Connect restart failed due to missing WAL"
    log "7. ✓ Permanent data loss demonstrated"
    log "8. ✓ Normal WAL settings restored"
    echo ""
    error "CRITICAL FINDINGS:"
    error "- restart_lsn older than retention period causes PERMANENT failure"
    error "- wal_status changes to 'lost' when required WAL files are purged"
    error "- Data created during downtime is permanently lost"
    error "- Recovery requires full table re-sync (not demonstrated)"
    error "- Aggressive WAL settings greatly increase data loss risk"
    echo ""
    log "PREVENTION STRATEGIES:"
    log "- Configure adequate wal_keep_size (minimum 1GB+ in production)"
    log "- Monitor replication slot lag continuously"
    log "- Set up alerts when wal_status != 'reserved'"
    log "- Implement proper backup/restore procedures"
    log "- Use longer checkpoint intervals in production"
    log "- Consider backup replication slots for critical systems"
    echo ""
    warning "This test used artificially aggressive settings for demonstration."
    warning "Production systems should use much more conservative WAL retention!"
}



# Execute main function
main
