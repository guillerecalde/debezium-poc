#!/bin/bash

# Script to consume CDC events from Kafka topics

echo "Available topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

echo ""
echo "Choose a topic to consume from:"
echo "1. customers"
echo "2. products"
echo "3. orders"
echo "4. All topics (in separate terminals)"

read -p "Enter your choice (1-4): " choice

case $choice in
    1)
        echo "Consuming from customers topic..."
        docker exec -it kafka kafka-console-consumer \
            --bootstrap-server localhost:29092 \
            --topic customers \
            --from-beginning \
            --property print.key=true \
            --property key.separator=" = "
        ;;
    2)
        echo "Consuming from products topic..."
        docker exec -it kafka kafka-console-consumer \
            --bootstrap-server localhost:29092 \
            --topic products \
            --from-beginning \
            --property print.key=true \
            --property key.separator=" = "
        ;;
    3)
        echo "Consuming from orders topic..."
        docker exec -it kafka kafka-console-consumer \
            --bootstrap-server localhost:29092 \
            --topic orders \
            --from-beginning \
            --property print.key=true \
            --property key.separator=" = "
        ;;
    4)
        echo "Open separate terminals and run:"
        echo "docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic customers --from-beginning"
        echo "docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic products --from-beginning"
        echo "docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic orders --from-beginning"
        ;;
    *)
        echo "Invalid choice"
        ;;
esac
