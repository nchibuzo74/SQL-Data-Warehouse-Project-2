/*
===============================================================================
Stored Procedure: Load Bronze Layer from Single File with Date Handling
===============================================================================
Script Purpose:
    This procedure handles date conversion issues by:
    1. Loading data into a staging table (mixed data types)
    2. Converting dates properly with date casting
    3. Inserting cleaned data into target table

    Parameters:
    file_path TEXT              - Full path to the CSV file to load
    p_table_name TEXT           - Name of the bronze table (without schema prefix)
    append_mode BOOLEAN         - TRUE = Append data (default), FALSE = Truncate first
    first_row INT               - Row number where data starts (default = 2, but use HEADER)
    field_terminator CHAR(1)    - Field delimiter (default = ',')

Usage Examples:
    -- Append new data to existing table (customer table)
    CALL bronze.load_bronze_file(
        'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_crm/cust_info_one.csv',
        'crm_cust_info',
        TRUE
    );

    -- Append new data to existing table (product table)
    CALL bronze.load_bronze_file(
        'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_crm/prd_info_one.csv',
        'crm_prd_info',
        TRUE
    );

    -- Append new data to existing table (sales table)
    CALL bronze.load_bronze_file(
        'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_crm/sales_details_one.csv',
        'crm_sales_details',
        TRUE
    );

    -- Append new data to existing table (ERP location table)
    CALL bronze.load_bronze_file(
        'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_erp/LOC_A101_one.csv',
        'erp_loc_a101',
        TRUE
    );

    -- Append new data to existing table (ERP customer birth table)
    CALL bronze.load_bronze_file(
        'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_erp/CUST_AZ12_one.csv',
        'erp_cust_az12',
        TRUE
    );

    -- Replace all data in table (full refresh example)
    CALL bronze.load_bronze_file(
        'C:/Chibz/VS Code Project/SQL Data Warehouse Project/datasets/source_crm/cust_info.csv',
        'crm_cust_info',
        FALSE
    );

===============================================================================
*/

/*
===============================================================================
Stored Procedure: Load Bronze Layer from Single File with Date Handling
===============================================================================
*/

-- Drop the existing procedure first if needed
---DROP PROCEDURE IF EXISTS bronze.load_bronze_file(TEXT, TEXT, BOOLEAN, INT, CHAR);

-- Now create with the corrected parameter name
CREATE OR REPLACE PROCEDURE bronze.load_bronze_file(
    file_path TEXT,
    p_table_name TEXT,
    append_mode BOOLEAN DEFAULT TRUE,
    first_row INT DEFAULT 2,
    field_terminator CHAR(1) DEFAULT ','
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    inserted_count INT;
    full_table_name TEXT;
    staging_table_name TEXT;
    duration_seconds INT;
BEGIN
    start_time := CLOCK_TIMESTAMP();
    full_table_name := 'bronze.' || p_table_name;
    staging_table_name := p_table_name || '_staging';
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading File with Data Type Handling';
    RAISE NOTICE '================================================';
    RAISE NOTICE 'File Path: %', file_path;
    RAISE NOTICE 'Target Table: %', full_table_name;
    RAISE NOTICE 'Staging Table: bronze.%', staging_table_name;
    RAISE NOTICE 'Mode: %', CASE WHEN append_mode THEN 'APPEND' ELSE 'REPLACE' END;
    RAISE NOTICE '------------------------------------------------';
    
    -- Validate tables exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = p_table_name
    ) THEN
        RAISE EXCEPTION 'Target table % does not exist', full_table_name;
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = staging_table_name
    ) THEN
        RAISE EXCEPTION 'Staging table bronze.% does not exist. Please create it first.', staging_table_name;
    END IF;
    
    -- STEP 1: Clear staging table
    RAISE NOTICE '>> Step 1: Clearing staging table';
    EXECUTE format('TRUNCATE TABLE bronze.%I', staging_table_name);
    
    -- STEP 2: Load to staging
    RAISE NOTICE '>> Step 2: Loading data to staging';
    EXECUTE format('COPY bronze.%I FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER %L)', 
                   staging_table_name, file_path, field_terminator);
    
    EXECUTE format('SELECT COUNT(*) FROM bronze.%I', staging_table_name) INTO row_count;
    RAISE NOTICE '   - Loaded % rows to staging', row_count;
    
    -- STEP 3: Truncate target if replace mode
    IF NOT append_mode THEN
        RAISE NOTICE '>> Step 3: Truncating target table (REPLACE mode)';
        EXECUTE format('TRUNCATE TABLE bronze.%I', p_table_name);
    ELSE
        RAISE NOTICE '>> Step 3: Skipping truncate (APPEND mode)';
    END IF;
    
    -- STEP 4: Insert with proper type conversion
    RAISE NOTICE '>> Step 4: Converting data types and inserting to target table';
    
    -- ===========================
    -- CRM Tables
    -- ===========================
    IF p_table_name = 'crm_cust_info' THEN
        -- Staging has: VARCHAR columns - convert cst_id to INT and cst_create_date to DATE
        INSERT INTO bronze.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, 
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            CASE WHEN TRIM(cst_id) = '' THEN NULL ELSE cst_id::INTEGER END,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            -- Convert VARCHAR date to DATE
            CASE 
                WHEN TRIM(COALESCE(cst_create_date, '')) = '' THEN NULL
                ELSE COALESCE(
                    CASE WHEN cst_create_date ~ '^\d{2}/\d{2}/\d{4}' 
                         THEN TO_DATE(cst_create_date, 'MM/DD/YYYY') END,
                    CASE WHEN cst_create_date ~ '^\d{4}-\d{2}-\d{2}' 
                         THEN TO_DATE(cst_create_date, 'YYYY-MM-DD') END,
                    NULL
                )
            END AS cst_create_date
        FROM bronze.crm_cust_info_staging
        WHERE TRIM(COALESCE(cst_id, '')) != '';
        
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        
    ELSIF p_table_name = 'crm_prd_info' THEN
        -- Staging has: INT for prd_id and prd_cost, TIMESTAMP for dates - use directly
        INSERT INTO bronze.crm_prd_info (
            prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt
        )
        SELECT 
            prd_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt  -- Already TIMESTAMP in staging
        FROM bronze.crm_prd_info_staging
        WHERE prd_id IS NOT NULL;
        
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        
    ELSIF p_table_name = 'crm_sales_details' THEN
        -- Staging has: INT columns already - use directly
        INSERT INTO bronze.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, 
            sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,      -- Already INT
            sls_order_dt,     -- Already INT (YYYYMMDD format)
            sls_ship_dt,      -- Already INT
            sls_due_dt,       -- Already INT
            sls_sales,        -- Already INT
            sls_quantity,     -- Already INT
            sls_price         -- Already INT
        FROM bronze.crm_sales_details_staging
        WHERE TRIM(COALESCE(sls_ord_num, '')) != '';
        
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        
    -- ===========================
    -- ERP Tables
    -- ===========================
    ELSIF p_table_name = 'erp_loc_a101' THEN
        -- Staging has: VARCHAR columns - use directly
        INSERT INTO bronze.erp_loc_a101 (cid, cntry)
        SELECT 
            cid,
            cntry
        FROM bronze.erp_loc_a101_staging
        WHERE TRIM(COALESCE(cid, '')) != '';
        
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        
    ELSIF p_table_name = 'erp_cust_az12' THEN
        -- Staging has: VARCHAR for cid/gen, TIMESTAMP for bdate - use directly
        INSERT INTO bronze.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            cid,
            bdate,  -- Already VARCHAR in staging
            gen
        FROM bronze.erp_cust_az12_staging
        WHERE TRIM(COALESCE(cid, '')) != '';
        
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        
    ELSIF p_table_name = 'erp_px_cat_g1v2' THEN
        -- Staging has: VARCHAR columns - convert maintenance to NUMERIC
        INSERT INTO bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT 
            id,
            cat,
            subcat,
            CASE WHEN TRIM(COALESCE(maintenance, '')) = '' THEN NULL 
                 ELSE maintenance::NUMERIC(19,2) END
        FROM bronze.erp_px_cat_g1v2_staging
        WHERE TRIM(COALESCE(id, '')) != '';
        
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        
    ELSE
        RAISE EXCEPTION 'Table "%" is not supported. Supported tables: crm_cust_info, crm_prd_info, crm_sales_details, erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2', p_table_name;
    END IF;
    
    RAISE NOTICE '   - Inserted % rows to target table', inserted_count;
    
    -- STEP 5: Clear staging
    RAISE NOTICE '>> Step 5: Clearing staging table';
    EXECUTE format('TRUNCATE TABLE bronze.%I', staging_table_name);
    
    end_time := CLOCK_TIMESTAMP();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    
    RAISE NOTICE '>> Total Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '================================================';
    RAISE NOTICE 'File Load Completed Successfully';
    RAISE NOTICE '================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================================';
        RAISE NOTICE 'ERROR OCCURRED DURING FILE LOAD';
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '================================================';
        
        -- Re-raise the error to rollback transaction
        RAISE;
END;
$$;
