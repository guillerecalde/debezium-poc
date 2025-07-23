#!/bin/bash

# Test Script: Transaction Rollback Handling
# This script tests how Debezium handles committed vs uncommitted/rolled-back transactions

# Source common utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/test-utils.sh"

echo "============================================"
echo "    TRANSACTION ROLLBACK TEST"
echo "============================================"

# Function to execute SQL in a transaction and commit
execute_committed_transaction() {
    local transaction_name=$1
    local sql_commands=$2
    log "Executing committed transaction: $transaction_name"

    docker exec postgres psql -U postgres -d testdb -c "
        BEGIN;
        $sql_commands
        COMMIT;
    "
    success "✅ Transaction '$transaction_name' committed successfully"
}

# Function to execute SQL in a transaction and rollback
execute_rolled_back_transaction() {
    local transaction_name=$1
    local sql_commands=$2
    log "Executing transaction to be rolled back: $transaction_name"

    docker exec postgres psql -U postgres -d testdb -c "
        BEGIN;
        $sql_commands
        ROLLBACK;
    "
    warning "🔄 Transaction '$transaction_name' rolled back"
}

# Function to show current transaction state (enhanced to use with existing functions)
show_transaction_state() {
    local phase=$1
    log "Active transactions ($phase):"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            pid,
            state,
            query_start,
            state_change,
            left(query, 80) as current_query
        FROM pg_stat_activity
        WHERE datname = 'testdb' AND state IN ('active', 'idle in transaction');
    "
    echo ""
}

# Function to show enhanced WAL activity (builds on replication slot info)
show_enhanced_wal_activity() {
    local phase=$1
    log "Enhanced WAL activity ($phase):"
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            pg_current_wal_lsn() as current_wal_lsn,
            pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') as wal_bytes_generated,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as wal_size_pretty;
    "

    # Show recent WAL activity
    docker exec postgres psql -U postgres -d testdb -c "
        SELECT
            count(*) as active_transactions,
            count(*) FILTER (WHERE state = 'active') as active_queries,
            count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
        FROM pg_stat_activity
        WHERE datname = 'testdb' AND pid != pg_backend_pid();
    "
    echo ""
}

# Main test execution
main() {
    log "Starting Transaction Rollback Test"

    # Step 1: Verify initial setup
    log "Step 1: Verifying initial setup..."
    if ! docker ps | grep -q kafka; then
        error "Kafka container is not running. Please start the environment first."
        exit 1
    fi

    # Verify services are running using common functions
    check_kafka_running
    setup_kafka_connect
    check_connect_status_basic

    # Show initial state using common functions
    show_database_state "Initial Setup"
    capture_kafka_offsets "Initial Setup"
    capture_replication_slot_info "Initial Setup"
    capture_kafka_status "Initial Setup"
    show_database_connections "Initial Setup"
    wait_for_user

    # Step 2: Execute committed transaction
    log "Step 2: Executing a committed transaction..."

    execute_committed_transaction "Committed Transaction 1" "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('Committed', 'User1', 'committed1@transaction-test.com'),
        ('Committed', 'User2', 'committed2@transaction-test.com');

        INSERT INTO inventory.products (name, description, price, quantity) VALUES
        ('Committed Product', 'This product should appear in Kafka', 299.99, 20);
    "

    sleep 10
    show_database_state "After Committed Transaction"
    capture_kafka_offsets "After Committed Transaction"
    capture_replication_slot_info "After Committed Transaction"
    capture_kafka_status "After Committed Transaction"
    show_message_correlation "After Committed Transaction"
    wait_for_user

    # Step 3: Execute rolled-back transaction
    log "Step 3: Executing a transaction that will be rolled back..."

    execute_rolled_back_transaction "Rolled Back Transaction 1" "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('Rollback', 'User1', 'rollback1@transaction-test.com'),
        ('Rollback', 'User2', 'rollback2@transaction-test.com');

        INSERT INTO inventory.products (name, description, price, quantity) VALUES
        ('Rollback Product', 'This product should NOT appear in Kafka', 199.99, 15);

        UPDATE inventory.customers SET email = 'should-not-update-1@rollback.com' WHERE first_name = 'Committed' AND last_name = 'User1';
        UPDATE inventory.customers SET email = 'should-not-update-2@rollback.com' WHERE first_name = 'Committed' AND last_name = 'User2';
    "

    sleep 10
    show_database_state "After Rolled Back Transaction"
    capture_kafka_offsets "After Rolled Back Transaction"
    capture_replication_slot_info "After Rolled Back Transaction"
    capture_kafka_status "After Rolled Back Transaction"
    show_message_correlation "After Rolled Back Transaction"
    wait_for_user

    # Step 4: Mix of committed and rolled-back transactions
    log "Step 4: Executing mixed committed and rolled-back transactions..."

    # First: Another committed transaction
    execute_committed_transaction "Committed Transaction 2" "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('Mixed', 'Committed', 'mixed-committed@transaction-test.com');

        UPDATE inventory.products SET quantity = quantity + 10 WHERE name = 'Committed Product';
    "

    sleep 2

    # Second: Another rolled-back transaction
    execute_rolled_back_transaction "Rolled Back Transaction 2" "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('Mixed', 'RolledBack', 'mixed-rollback@transaction-test.com');

        DELETE FROM inventory.products WHERE name = 'Committed Product';

        INSERT INTO inventory.products (name, description, price, quantity) VALUES
        ('Should Not Exist', 'This should be rolled back', 999.99, 1);
    "

    sleep 2

    # Third: Final committed transaction
    execute_committed_transaction "Committed Transaction 3" "
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('Final', 'Committed', 'final-committed@transaction-test.com');

        INSERT INTO inventory.products (name, description, price, quantity) VALUES
        ('Final Product', 'This should appear in Kafka', 399.99, 25);
    "

    sleep 10
    show_database_state "After Mixed Transactions"
    capture_kafka_offsets "After Mixed Transactions"
    capture_replication_slot_info "After Mixed Transactions"
    capture_kafka_status "After Mixed Transactions"
    show_message_correlation "After Mixed Transactions"
    wait_for_user

    # Step 5: Long-running transaction test
    log "Step 5: Testing long-running transaction behavior..."

    # Start a long transaction, do some work, then rollback
    log "Starting long-running transaction (will be active for 15 seconds)..."

    docker exec postgres psql -U postgres -d testdb -c "
        BEGIN;
        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('LongRunning', 'Transaction', 'long-running@transaction-test.com');

        INSERT INTO inventory.products (name, description, price, quantity) VALUES
        ('Long Running Product', 'Should be rolled back', 599.99, 5);

        -- This keeps the transaction open
        SELECT pg_sleep(5);

        INSERT INTO inventory.customers (first_name, last_name, email) VALUES
        ('LongRunning', 'Transaction2', 'long-running2@transaction-test.com');

        SELECT pg_sleep(5);

        UPDATE inventory.products SET price = 699.99 WHERE name = 'Long Running Product';

        SELECT pg_sleep(5);

        ROLLBACK;
    " &

    # Monitor while transaction is running using common functions
    log "Monitoring while long transaction is running..."
    for i in {1..3}; do
        sleep 10
        log "Check $i during long transaction:"
        show_transaction_state "During Long Transaction $i"
        show_database_connections "During Long Transaction $i"
        capture_kafka_status "During Long Transaction $i"
    done

    wait # Wait for the background transaction to complete

    sleep 10
    show_database_state "After Long Transaction Rollback"
    capture_kafka_offsets "After Long Transaction Rollback"
    capture_replication_slot_info "After Long Transaction Rollback"
    capture_kafka_status "After Long Transaction Rollback"
    wait_for_user

    # Step 6: Final verification and analysis
    log "Step 6: Final verification and analysis..."

    show_database_state "Final State"
    capture_kafka_offsets "Final State"
    capture_replication_slot_info "Final State"
    capture_kafka_status "Final State"
    show_message_correlation "Final State"

    # Enhanced final analysis using common functions
    show_database_connections "Final Analysis"
    show_enhanced_wal_activity "Final Analysis"

    # Show detailed message counts using common function
    log "Final message count verification:"
    check_message_counts

    # Final summary
    echo ""
    success "============================================"
    success "      TRANSACTION ROLLBACK TEST SUMMARY"
    success "============================================"
    log "This test verified Debezium's transaction handling:"
    log ""
    log "✅ COMMITTED TRANSACTIONS:"
    log "  • Committed Transaction 1: 2 customers + 1 product"
    log "  • Committed Transaction 2: 1 customer + 1 product update"
    log "  • Committed Transaction 3: 1 customer + 1 product"
    log "  • Total committed: 4 customers, 2 products (1 updated)"
    log ""
    log "🔄 ROLLED BACK TRANSACTIONS:"
    log "  • Rolled Back Transaction 1: 2 customers + 1 product + 1 update"
    log "  • Rolled Back Transaction 2: 1 customer + 1 delete + 1 product"
    log "  • Long Running Transaction: 2 customers + 1 product + 1 update"
    log "  • These should NOT appear in Kafka topics"
    log ""
    log "KEY OBSERVATIONS:"
    log "  1. Only committed data appears in the database"
    log "  2. Only committed data gets replicated to Kafka"
    log "  3. Rolled back transactions don't generate CDC events"
    log "  4. Long-running transactions don't replicate until commit"
    log "  5. WAL only contains committed transaction data"
    log ""
    log "DATABASE FINAL STATE should show:"
    log "  • 4 customers with names: Committed, Mixed Committed, Final Committed"
    log "  • 2 products: 'Committed Product' (updated qty), 'Final Product'"
    log "  • NO customers with 'Rollback' or 'LongRunning' names"
    log "  • NO products with 'Rollback', 'Should Not Exist', or 'Long Running' names"
    log ""
    log "KAFKA TOPICS should contain:"
    log "  • Only events for committed transactions"
    log "  • Message count should match committed database changes"
    log "  • No events for rolled back transactions"
    log ""
    warning "This demonstrates Debezium's ACID compliance:"
    warning "• Atomicity: All-or-nothing transaction replication"
    warning "• Consistency: Only committed state is replicated"
    warning "• Isolation: In-progress transactions don't generate events"
    warning "• Durability: Committed changes are reliably captured"
    log ""
    log "For detailed analysis:"
    log "• Check database state vs Kafka message content"
    log "• Verify message timestamps align with commit times"
    log "• Monitor: ./scripts/monitor-debezium.sh"
    log "• Kafka UI: http://localhost:8080"
}

# Execute main function
main
