-- Data Warehouse Schema Initialization
-- Creates raw layer schemas and tables for ingestion

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw layer tables (ingestion target)
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    country VARCHAR(100),
    created_at TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    order_date TIMESTAMP,
    status VARCHAR(50),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR(100)
);

-- Create indexes for query performance
CREATE INDEX IF NOT EXISTS idx_raw_customers_id ON raw.customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_products_id ON raw.products(product_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_id ON raw.orders(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_customer ON raw.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_product ON raw.orders(product_id);

-- Grant permissions (for dbt user)
GRANT USAGE ON SCHEMA raw TO pipeline;
GRANT USAGE ON SCHEMA staging TO pipeline;
GRANT USAGE ON SCHEMA intermediate TO pipeline;
GRANT USAGE ON SCHEMA marts TO pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA intermediate TO pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA intermediate TO pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA marts TO pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT ALL ON TABLES TO pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO pipeline;
