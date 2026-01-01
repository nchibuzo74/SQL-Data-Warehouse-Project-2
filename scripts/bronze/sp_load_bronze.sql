/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

-- DROP PROCEDURE IF EXISTS bronze.load_bronze;

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    duration_seconds INTEGER;
    batch_duration_seconds INTEGER;
BEGIN
    -- Start timing
    batch_start_time := CLOCK_TIMESTAMP();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- Load crm_cust_info
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_crm/cust_info.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    
    end_time := CLOCK_TIMESTAMP();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> -------------';

    -- Load crm_prd_info
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_crm/prd_info.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    
    end_time := CLOCK_TIMESTAMP();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> -------------';

    -- Load crm_sales_details
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_crm/sales_details.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    
    end_time := CLOCK_TIMESTAMP();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> -------------';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- Load erp_loc_a101
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM 'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_erp/LOC_A101.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    
    end_time := CLOCK_TIMESTAMP();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> -------------';

    -- Load erp_cust_az12
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM 'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    
    end_time := CLOCK_TIMESTAMP();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> -------------';

    -- Load erp_px_cat_g1v2
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM 'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    
    end_time := CLOCK_TIMESTAMP();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> -------------';

    batch_end_time := CLOCK_TIMESTAMP();
    batch_duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time))::INTEGER;
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', batch_duration_seconds;
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
        
        -- Re-raise the error
        RAISE;
END;
$$;
