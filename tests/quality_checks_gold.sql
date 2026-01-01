/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers' and 'gold.customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers and gold.customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key' and 'gold.products'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales' and 'gold.sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT f.* 
FROM gold.fact_sales as f
LEFT JOIN gold.dim_customers as c
ON f.customer_key = c.customer_number
LEFT JOIN gold.dim_products as p
ON f.product_key = p.product_number
WHERE p.product_key IS NULL OR c.customer_key IS NULL;

SELECT f.* 
FROM gold.sales as f
LEFT JOIN gold.customers as c
ON f.customer_number = c.customer_number
LEFT JOIN gold.products as p
ON f.product_number = p.product_number
WHERE p.product_key IS NULL OR c.customer_key IS NULL;
