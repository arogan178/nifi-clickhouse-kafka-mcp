-- The second lock: the gateway's user can only read, and can't change that.
CREATE USER mcp IDENTIFIED BY 'mcp-password' SETTINGS readonly = 2, max_execution_time = 60 READONLY;
CREATE DATABASE shop;
CREATE TABLE shop.orders (id UInt64, customer_email String, amount Decimal(10, 2), created DateTime)
ENGINE = MergeTree ORDER BY id COMMENT 'Customer orders';
INSERT INTO shop.orders SELECT number, concat('user', toString(number % 50), '@example.com'),
    (number % 97) + 0.99, now() - number * 60 FROM numbers(5000);
GRANT SELECT ON shop.* TO mcp;
GRANT SELECT ON system.* TO mcp;
