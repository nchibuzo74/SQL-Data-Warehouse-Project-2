/*
===============================================================================
Stored Procedure: Create Staging Tables for Incremental Loads with Date Handling as Text the way Source Provides (CSV Files).
===============================================================================
*/

-- ============================================================================
-- STEP 1: Create ALL Staging Tables (Run Once)
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Creating Staging Tables';
    RAISE NOTICE '================================================';
END $$;

-- ===========================
-- CRM Staging Tables
-- ===========================

-- Staging table for crm_cust_info (all columns as TEXT)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = 'crm_cust_info_staging'
    ) THEN
        CREATE TABLE bronze.crm_cust_info_staging (
            cst_id VARCHAR(50),
            cst_key varchar(50),
            cst_firstname varchar(50),
            cst_lastname varchar(50),
            cst_marital_status varchar(50),
            cst_gndr varchar(50),
            cst_create_date VARCHAR(50)
        );
        RAISE NOTICE '✓ Created staging table: bronze.crm_cust_info_staging';
    ELSE
        RAISE NOTICE '• Staging table already exists: bronze.crm_cust_info_staging';
    END IF;
END $$;

-- Staging table for crm_prd_info
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = 'crm_prd_info_staging'
    ) THEN
        CREATE TABLE bronze.crm_prd_info_staging (
        prd_id       INT,
        prd_key      VARCHAR(50),
        prd_nm       VARCHAR(50),
        prd_cost     INT,
        prd_line     VARCHAR(50),
        prd_start_dt TIMESTAMP,
        prd_end_dt   TIMESTAMP
        );
        RAISE NOTICE '✓ Created staging table: bronze.crm_prd_info_staging';
    ELSE
        RAISE NOTICE '• Staging table already exists: bronze.crm_prd_info_staging';
    END IF;
END $$;

-- Staging table for crm_sales_details
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = 'crm_sales_details_staging'
    ) THEN
        CREATE TABLE bronze.crm_sales_details_staging (
            sls_ord_num  VARCHAR(50),
            sls_prd_key  VARCHAR(50),
            sls_cust_id  INT,
            sls_order_dt INT,
            sls_ship_dt  INT,
            sls_due_dt   INT,
            sls_sales    INT,
            sls_quantity INT,
            sls_price    INT
        );
        RAISE NOTICE '✓ Created staging table: bronze.crm_sales_details_staging';
    ELSE
        RAISE NOTICE '• Staging table already exists: bronze.crm_sales_details_staging';
    END IF;
END $$;

-- ===========================
-- ERP Staging Tables
-- ===========================

-- Staging table for erp_loc_a101
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = 'erp_loc_a101_staging'
    ) THEN
        CREATE TABLE bronze.erp_loc_a101_staging (
        cid    VARCHAR(50),
        cntry  VARCHAR(50)
        );
        RAISE NOTICE '✓ Created staging table: bronze.erp_loc_a101_staging';
    ELSE
        RAISE NOTICE '• Staging table already exists: bronze.erp_loc_a101_staging';
    END IF;
END $$;

-- Staging table for erp_cust_az12
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = 'erp_cust_az12_staging'
    ) THEN
        CREATE TABLE bronze.erp_cust_az12_staging (
        cid    VARCHAR(50),
        bdate  VARCHAR(50),
        gen    VARCHAR(10)
        );
        RAISE NOTICE '✓ Created staging table: bronze.erp_cust_az12_staging';
    ELSE
        RAISE NOTICE '• Staging table already exists: bronze.erp_cust_az12_staging';
    END IF;
END $$;

-- Staging table for erp_px_cat_g1v2
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = 'erp_px_cat_g1v2_staging'
    ) THEN
        CREATE TABLE bronze.erp_px_cat_g1v2_staging (
            id           VARCHAR(50),
            cat          VARCHAR(50),
            subcat       VARCHAR(50),
            maintenance  VARCHAR(10)
                );
        RAISE NOTICE '✓ Created staging table: bronze.erp_px_cat_g1v2_staging';
    ELSE
        RAISE NOTICE '• Staging table already exists: bronze.erp_px_cat_g1v2_staging';
    END IF;
END $$;

DO $$
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Staging Tables Setup Complete';
    RAISE NOTICE '================================================';
END $$;
