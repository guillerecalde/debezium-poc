# Debezium CDC with PostgreSQL and Kafka

This repository contains a complete Docker Compose setup for testing Debezium Change Data Capture (CDC) with PostgreSQL and Kafka.

## Architecture

The setup includes the following components:

- **Zookeeper**: Coordination service for Kafka
- **Kafka**: Message broker for streaming data
- **PostgreSQL**: Database with logical replication enabled for CDC
- **Kafka Connect with Debezium**: CDC connector to capture database changes
- **Kafka UI**: Web interface to monitor Kafka topics and connectors

## Prerequisites

- Docker and Docker Compose installed
- curl (for API calls)
- Optional: PostgreSQL client (psql) for testing

## Quick Start

### 1. Start the Environment

```bash
docker-compose up -d
```

This will start all services in the background. The initial startup may take a few minutes as Docker pulls the required images.

### 2. Verify Services are Running

Check that all services are healthy:

```bash
docker-compose ps
```

You should see all services in "Up" status.

### 3. Set up the Debezium Connector

Wait for all services to be ready (about 2-3 minutes), then run:

```bash
./scripts/setup-connector.sh
```

This script will:
- Wait for Kafka Connect to be ready
- Create the Debezium PostgreSQL connector
- Configure CDC for the inventory tables

### 4. Verify the Connector

Check connector status:

```bash
curl localhost:8083/connectors/inventory-connector/status
```

You should see the connector in "RUNNING" state.

## Testing CDC

### View Initial Data

The PostgreSQL database is pre-populated with sample data in three tables:
- `inventory.customers`
- `inventory.products`
- `inventory.orders`

### Monitor Kafka Topics

You can monitor CDC events in several ways:

#### Option 1: Using Kafka UI (Recommended)
Open http://localhost:8080 in your browser to view:
- Topics and their messages
- Connector status
- Consumer groups

#### Option 2: Using the Console Consumer Script
```bash
./scripts/consume-events.sh
```

#### Option 3: Manual Console Consumer
```bash
# Consume customers events
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:29092 \
    --topic customers \
    --from-beginning

# Consume products events
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:29092 \
    --topic products \
    --from-beginning

# Consume orders events
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:29092 \
    --topic orders \
    --from-beginning
```

### Generate Test Events

#### Option 1: Using the Test SQL Script
Connect to PostgreSQL and run the test operations:

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d testdb

# Or if you have psql installed locally
psql -h localhost -U postgres -d testdb

# Run the test script
\i scripts/test-cdc.sql
```

#### Option 2: Manual Operations
```sql
-- Insert new customer
INSERT INTO inventory.customers (first_name, last_name, email) VALUES
    ('Alice', 'Williams', 'alice.williams@example.com');

-- Update existing customer
UPDATE inventory.customers
SET email = 'john.doe.updated@example.com'
WHERE id = 1;

-- Delete a record
DELETE FROM inventory.orders WHERE id = 1;
```

Each of these operations will generate corresponding CDC events in Kafka.

## Service Endpoints

- **Kafka UI**: http://localhost:8080
- **Kafka Connect REST API**: http://localhost:8083
- **PostgreSQL**: localhost:5432 (postgres/postgres)
- **Kafka**: localhost:9092

## Understanding CDC Events

Debezium generates different types of events:

### Insert Event (op: "c" for create)
```json
{
  "op": "c",
  "ts_ms": 1234567890,
  "before": null,
  "after": {
    "id": 4,
    "first_name": "Alice",
    "last_name": "Williams",
    "email": "alice.williams@example.com"
  }
}
```

### Update Event (op: "u" for update)
```json
{
  "op": "u",
  "ts_ms": 1234567890,
  "before": {
    "id": 1,
    "email": "john.doe@example.com"
  },
  "after": {
    "id": 1,
    "email": "john.doe.updated@example.com"
  }
}
```

### Delete Event (op: "d" for delete)
```json
{
  "op": "d",
  "ts_ms": 1234567890,
  "before": {
    "id": 1,
    "first_name": "John",
    "last_name": "Doe"
  },
  "after": null
}
```

## Useful Commands

### Connector Management
```bash
# List all connectors
curl localhost:8083/connectors

# Get connector status
curl localhost:8083/connectors/inventory-connector/status

# Delete connector
curl -X DELETE localhost:8083/connectors/inventory-connector

# Restart connector
curl -X POST localhost:8083/connectors/inventory-connector/restart
```

### Kafka Topic Management
```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Describe a topic
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic customers

# Check consumer group offsets
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --list
```

### PostgreSQL Operations
```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d testdb

# Check replication slots
SELECT * FROM pg_replication_slots;

# Check publications (if using pgoutput)
SELECT * FROM pg_publication;
```

## Configuration Details

### PostgreSQL Configuration
The PostgreSQL container is configured with:
- `wal_level=logical`: Enables logical replication
- `max_wal_senders=1`: Allows one WAL sender
- `max_replication_slots=1`: Allows one replication slot

### Debezium Connector Configuration
Key configuration parameters:
- **plugin.name**: `pgoutput` (PostgreSQL's built-in logical replication)
- **slot.name**: `debezium_slot` (replication slot name)
- **publication.autocreate.mode**: `filtered` (auto-create publications)
- **table.include.list**: Only monitor specific tables
- **transforms.route**: Simplify topic names (removes schema prefix)

## Troubleshooting

### Services Won't Start
```bash
# Check logs
docker-compose logs [service-name]

# Common issues:
docker-compose logs kafka-connect
docker-compose logs postgres
```

### Connector Issues
```bash
# Check connector logs
docker-compose logs kafka-connect

# Restart connector
curl -X POST localhost:8083/connectors/inventory-connector/restart
```

### No CDC Events
1. Verify connector is running: `curl localhost:8083/connectors/inventory-connector/status`
2. Check if replication slot exists: `SELECT * FROM pg_replication_slots;`
3. Ensure tables are in the include list
4. Verify PostgreSQL logical replication is enabled

## Cleanup

To stop all services and remove volumes:

```bash
docker-compose down -v
```

To stop services but keep data:

```bash
docker-compose down
```

## Advanced Usage

### Custom Table Monitoring
To monitor additional tables, update the `table.include.list` in `debezium-postgres-connector.json`:

```json
{
  "table.include.list": "inventory.customers,inventory.products,inventory.orders,inventory.your_new_table"
}
```

### Schema Evolution
Debezium automatically handles schema changes. When you add/remove columns, the CDC events will reflect the new schema.

### Filtering Events
You can add transforms to filter or modify events before they reach Kafka topics. See the Debezium documentation for more transform options.

## Resources

- [Debezium Documentation](https://debezium.io/documentation/)
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
