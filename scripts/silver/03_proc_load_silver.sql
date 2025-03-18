/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
Usage Example:
    CALL silver.load_silver();
===============================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    error_message TEXT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
    batch_end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    -- Create or replace necessary functions
    BEGIN
        RAISE NOTICE '>> Creating helper function: silver.convert_to_valid_date';
        
        CREATE OR REPLACE FUNCTION silver.convert_to_valid_date(input_val NUMERIC)
        RETURNS DATE AS $$
        BEGIN
            RETURN CASE 
                WHEN input_val <= 0 OR FLOOR(LOG(10, input_val::BIGINT)) + 1 != 8 
                THEN NULL
                ELSE TO_DATE(input_val::TEXT, 'YYYYMMDD') 
            END;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
        RAISE WARNING 'Failed to create function silver.convert_to_valid_date: %', error_message;
    END;

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading Silver CRM Tables';
    RAISE NOTICE '------------------------------------------------';
    
    -- Silver CRM Customer Info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        
        RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
        
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        WITH t AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE UPPER(TRIM(cst_marital_status)) 
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'N/A'
            END cst_marital_status,
            CASE UPPER(TRIM(cst_gndr)) 
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'N/A'
            END cst_gndr,
            cst_create_date
        FROM t
        WHERE flag_last = 1;
        
        GET DIAGNOSTICS error_message = ROW_COUNT;
        RAISE NOTICE '>> Rows Inserted: %', error_message;
        
        end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
        RAISE WARNING 'Failed to load silver.crm_cust_info: %', error_message;
    END;

    -- Silver CRM Product Info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        
        RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
        
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id, 
            prd_key,
            prd_nm,
            prd_cost, 
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A' 
            END AS prd_line,
            prd_start_dt,
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key 
                ORDER BY prd_start_dt
            ) - 1 AS prd_end_dt
        FROM bronze.crm_prd_info
        WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
            SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details
        );
        
        GET DIAGNOSTICS error_message = ROW_COUNT;
        RAISE NOTICE '>> Rows Inserted: %', error_message;
        
        end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
        RAISE WARNING 'Failed to load silver.crm_prd_info: %', error_message;
    END;

    -- Silver CRM Sales Details
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        
        RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
        
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            silver.convert_to_valid_date(sls_order_dt) AS sls_order_dt,
            silver.convert_to_valid_date(sls_ship_dt) AS sls_ship_dt,
            silver.convert_to_valid_date(sls_due_dt) AS sls_due_dt,
            -- sls_sales
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            -- sls_price
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;
        
        GET DIAGNOSTICS error_message = ROW_COUNT;
        RAISE NOTICE '>> Rows Inserted: %', error_message;
        
        end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
        RAISE WARNING 'Failed to load silver.crm_sales_details: %', error_message;
    END;

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading Silver ERP Tables';
    RAISE NOTICE '------------------------------------------------';
    
    -- Silver ERP Customer AZ12
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        
        RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
        
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
                ELSE cid
            END AS cid,
            CASE WHEN bdate > CURRENT_DATE THEN NULL
                ELSE bdate
            END AS bdate,
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'N/A'
            END AS gen
        FROM bronze.erp_cust_az12;
        
        GET DIAGNOSTICS error_message = ROW_COUNT;
        RAISE NOTICE '>> Rows Inserted: %', error_message;
        
        end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
        RAISE WARNING 'Failed to load silver.erp_cust_az12: %', error_message;
    END;

    -- Silver ERP Location A101
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        
        RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
        
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        
        GET DIAGNOSTICS error_message = ROW_COUNT;
        RAISE NOTICE '>> Rows Inserted: %', error_message;
        
        end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
        RAISE WARNING 'Failed to load silver.erp_loc_a101: %', error_message;
    END;

    -- Silver ERP Product Category G1V2
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        
        RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;
        
        GET DIAGNOSTICS error_message = ROW_COUNT;
        RAISE NOTICE '>> Rows Inserted: %', error_message;
        
        end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
        RAISE WARNING 'Failed to load silver.erp_px_cat_g1v2: %', error_message;
    END;
    
    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';
END;
$procedure$