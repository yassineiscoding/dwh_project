/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'datawarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'datawarehouse' database
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datawarehouse') THEN
        PERFORM dblink_exec('dbname=postgres', 'DROP DATABASE datawarehouse');
    END IF;
END $$;

-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;

-- Connect to the 'datawarehouse' database
\c datawarehouse

-- Create Schemas
CREATE SCHEMA gold;

-- Create Tables
CREATE TABLE gold.dim_customers(
    customer_key INTEGER,
    customer_id INTEGER,
    customer_number VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    country VARCHAR(50),
    marital_status VARCHAR(50),
    gender VARCHAR(50),
    birthdate DATE,
    create_date DATE
);

CREATE TABLE gold.dim_products(
    product_key INTEGER,
    product_id INTEGER,
    product_number VARCHAR(50),
    product_name VARCHAR(50),
    category_id VARCHAR(50),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    maintenance VARCHAR(50),
    cost INTEGER,
    product_line VARCHAR(50),
    start_date DATE
);

CREATE TABLE gold.fact_sales(
    order_number VARCHAR(50),
    product_key INTEGER,
    customer_key INTEGER,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount INTEGER,
    quantity SMALLINT,
    price INTEGER
);

-- Truncate and Insert Data
TRUNCATE TABLE gold.dim_customers;

COPY gold.dim_customers
FROM '/path/to/your/csv/gold.dim_customers.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE gold.dim_products;

COPY gold.dim_products
FROM '/path/to/your/csv/gold.dim_products.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE gold.fact_sales;

COPY gold.fact_sales
FROM '/path/to/your/csv/gold.fact_sales.csv'
DELIMITER ','
CSV HEADER;
