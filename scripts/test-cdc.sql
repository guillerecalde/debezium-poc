-- Test CDC operations
-- Connect to PostgreSQL: psql -h localhost -U postgres -d testdb

-- 1. Insert new customer
INSERT INTO inventory.customers (first_name, last_name, email) VALUES
    ('Alice', 'Williams', 'alice.williams@example.com');

-- 2. Update existing customer
UPDATE inventory.customers
SET email = 'john.doe.updated@example.com'
WHERE id = 1;

-- 3. Insert new product
INSERT INTO inventory.products (name, description, price, quantity) VALUES
    ('Tablet', 'High-resolution tablet', 599.99, 25);

-- 4. Update product quantity
UPDATE inventory.products
SET quantity = quantity - 5
WHERE id = 1;

-- 5. Create new order
INSERT INTO inventory.orders (customer_id, product_id, quantity, total_amount, status) VALUES
    (4, 4, 1, 599.99, 'pending');

-- 6. Update order status
UPDATE inventory.orders
SET status = 'shipped'
WHERE id = 2;

-- 7. Delete an order (this will generate a DELETE event)
DELETE FROM inventory.orders WHERE id = 3;

-- Check current data
SELECT 'CUSTOMERS' as table_name;
SELECT * FROM inventory.customers;

SELECT 'PRODUCTS' as table_name;
SELECT * FROM inventory.products;

SELECT 'ORDERS' as table_name;
SELECT * FROM inventory.orders;
