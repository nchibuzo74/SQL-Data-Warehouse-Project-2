/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates tables for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each table performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These tables can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.customers
-- =============================================================================

-- DROP TABLE IF EXISTS gold.customers;

DROP table IF EXISTS gold.customers;

CREATE TABLE gold.customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                     AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')               -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birth_date,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;

--------------------------------------------------------------
-- Add PRIMARY KEY constraint after table creation
ALTER TABLE gold.customers ADD PRIMARY KEY (customer_id);

-- Add UNIQUE constraint on customer_number
ALTER TABLE gold.customers ADD CONSTRAINT uk_customers_customer_number UNIQUE (customer_number);

-- Add UNIQUE constraint on customer_key
ALTER TABLE gold.customers ADD CONSTRAINT uk_customers_customer_key UNIQUE (customer_key);


-- Check existing indexes on the table
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'customers' 
AND schemaname = 'gold';

----- Composite index (if you often join by customer_number or filter by multiple columns together)
        ----The columns are country_number, country and gender
create index idx_customers_customer_number_country_gender on gold.customers(customer_number,country,gender);


-- =============================================================================
-- Create Dimension: gold.products
-- =============================================================================

DROP TABLE IF EXISTS gold.products;

CREATE TABLE gold.products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

---------------------------------------------------------------------
-- Add PRIMARY KEY constraint after table creation
alter table gold.products add PRIMARY KEY(product_id);

-- Add UNIQUE constraint on Product_number
ALTER TABLE gold.products ADD CONSTRAINT uk_products_product_number UNIQUE (product_number);

-- Add UNIQUE constraint on Product_Key
ALTER TABLE gold.products ADD CONSTRAINT uk_products_product_key UNIQUE (product_key);

-- Check existing indexes on the table
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'products' 
AND schemaname = 'gold';

----- Composite index (if you often join by product_number or filter by multiple columns together)
        ----The columns are product_number, category and subcategory
create index idx_products_product_number_category_subcategory on gold.products(product_number,category,subcategory);


-- =============================================================================
-- Create Fact Table: gold.sales
-- =============================================================================

DROP TABLE IF EXISTS gold.sales;

CREATE TABLE gold.sales AS
SELECT
    row_number() OVER (ORDER BY sd.sls_order_dt, sd.sls_ord_num) AS sales_key, -- Surrogate key
    sd.sls_ord_num  AS order_number,
    pr.prd_key      AS product_number,
    cu.cst_key      AS customer_number,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN silver.crm_prd_info AS pr
    ON sd.sls_prd_key = pr.prd_key
LEFT JOIN silver.crm_cust_info AS cu
    ON sd.sls_cust_id = cu.cst_id::int;

---------------------------------------------------------------------
-- Add PRIMARY KEY constraint after table creation
alter table gold.sales add PRIMARY KEY(sales_key);

-- Add FOREIGN KEY constraint after table creation
alter table gold.sales 
    add CONSTRAINT fk_sales_customers FOREIGN KEY (customer_number) REFERENCES gold.customers(customer_number);

alter table gold.sales 
    add CONSTRAINT fk_sales_products FOREIGN KEY (product_number) REFERENCES gold.products(product_number);

-- Check existing indexes on the table
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'sales' 
AND schemaname = 'gold';

----- Composite index (if you often join or filter by multiple columns together)
        ----The columns are product_number, order_number and customer_number, shipping_date, order_date, due_date
create index idx_sales_product_number_order_number_customer_number_shipping_date_order_date_due_date on gold.sales(product_number,order_number,customer_number,shipping_date,order_date,due_date);
