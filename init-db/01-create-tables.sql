-- Create test schema and tables for Debezium CDC testing
CREATE SCHEMA IF NOT EXISTS inventory;

-- Create customers table
CREATE TABLE inventory.customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table
CREATE TABLE inventory.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE inventory.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES inventory.customers(id),
    product_id INTEGER REFERENCES inventory.products(id),
    quantity INTEGER NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO inventory.customers (first_name, last_name, email) VALUES
    ('John', 'Doe', 'john.doe@example.com'),
    ('Jane', 'Smith', 'jane.smith@example.com'),
    ('Bob', 'Johnson', 'bob.johnson@example.com');

INSERT INTO inventory.products (name, description, price, quantity) VALUES
    ('Laptop', 'High-performance laptop', 999.99, 50),
    ('Mouse', 'Wireless optical mouse', 29.99, 100),
    ('Keyboard', 'Mechanical keyboard', 79.99, 75);

INSERT INTO inventory.orders (customer_id, product_id, quantity, total_amount, status) VALUES
    (1, 1, 1, 999.99, 'completed'),
    (2, 2, 2, 59.98, 'pending'),
    (3, 3, 1, 79.99, 'shipped');

-- Create function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update the updated_at column
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE
    ON inventory.customers FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE
    ON inventory.products FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE
    ON inventory.orders FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
