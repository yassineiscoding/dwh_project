/*
===============================================================================
Quality Checks for CRM Source
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ============================================================================
-- >> crm_cust_info
-- ============================================================================

-- Check for Nulls & Duplicates in PK 
SELECT
    cst_id,
    COUNT(*)
FROM
    bronze.crm_cust_info
GROUP BY
    cst_id
HAVING
    COUNT(*) > 1
    OR cst_id IS NULL;

-- Process to Resolve
-- Step 1: Focus on one number 
SELECT * 
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- Step 2: Find the most recent cst_create_date from table
SELECT 
    *,
    ROW_NUMBER() OVER (
        PARTITION BY cst_id 
        ORDER BY cst_create_date DESC 
    ) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- Step 3: Show only flag_last = 1 values in table & remove others
SELECT 
    *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC 
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS subquery
WHERE flag_last = 1;

-- Check for Unwanted Spaces
-- Step 1: Find values that are not equal to their trimmed version
SELECT 
    cst_firstname
FROM 
    bronze.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname);

-- Step 2: Check for other columns

-- Data Standardization & Consistency (cst_gndr)
SELECT DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;

-- Step 1: Convert abbreviations to full names
SELECT 
    cst_gndr,
    CASE 
        WHEN cst_gndr = 'F' THEN 'Female'
        WHEN cst_gndr = 'M' THEN 'Male'
        ELSE 'Unknown'
    END AS standardized_gndr
FROM 
    bronze.crm_cust_info;

-- Step 2: Handle lower-case values
SELECT 
    cst_gndr,
    CASE 
        WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
        WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
        ELSE 'Unknown'
    END AS standardized_gndr
FROM 
    bronze.crm_cust_info;

-- Step 3: Remove unwanted spaces
SELECT 
    cst_gndr,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'Unknown'
    END AS standardized_gndr
FROM 
    bronze.crm_cust_info;

-- Data Standardization & Consistency (cst_marital_status)
SELECT 
    cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'Unknown'
    END AS standardized_marital_status
FROM 
    bronze.crm_cust_info;

-- Check the Data Type for bronze.crm_cust_info
SELECT 
    column_name, 
    data_type
FROM 
    information_schema.columns
WHERE 
    table_name = 'crm_cust_info' 
    AND table_schema = 'bronze';

-- ============================================================================
-- >> crm_sales_details
-- ============================================================================

-- Validate Sales, Quantity, and Price Columns
-- Ensure sales = quantity * price
-- Ensure values are not NULL, zero, or negative
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Apply Fixes for Sales, Quantity, and Price
SELECT DISTINCT
    sls_sales AS OLD_SALES,
    sls_quantity,
    sls_price AS OLD_PRICE,
    
    CASE 
        WHEN sls_price < 0 THEN ABS(sls_price)
        WHEN sls_price = 0 THEN NULLIF(sls_price, 0)
        WHEN sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price,
    
    CASE
        WHEN sls_sales IS NULL THEN ABS(sls_quantity) * COALESCE(ABS(sls_price), 0)
        WHEN sls_sales < 0 THEN ABS(sls_sales)
        WHEN sls_sales = 0 THEN ABS(sls_quantity) * COALESCE(ABS(sls_price), 0)
        WHEN ABS(sls_quantity) * ABS(sls_price) != sls_sales THEN ABS(sls_quantity) * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales
FROM bronze.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
    OR sls_price > sls_sales
    OR sls_quantity > sls_sales
ORDER BY sls_sales, sls_quantity, sls_price;

-- Check for Unwanted Spaces
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Validate Product and Customer IDs
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT sls_prd_key FROM bronze.crm_prd_info);

-- Validate Date Columns
-- Step 1: Check for invalid Dates
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;

-- Step 2: Ensure Dates have 8 digits
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8;

-- Step 3: Check Date Boundaries (Valid Date Range: 19000101 - 20500101)
SELECT NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE
    LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8
    OR sls_ship_dt < 19000101
    OR sls_ship_dt > 20500101;

-- Step 4: Ensure Logical Date Order (Order Date < Ship Date < Due Date)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- ============================================================================
-- >> ERP Customer Table (erp_cust_az12)
-- ============================================================================

-- Check all columns and ensure transformations are working correctly
SELECT
    cid,
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END AS transformed_cid,
    bdate,
    gen
FROM 
    bronze.erp_cust_az12
WHERE
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Check for invalid birth dates
SELECT
    bdate,
    gen
FROM 
    bronze.erp_cust_az12
WHERE
    bdate < '1924-02-01' OR bdate > TO_DATE('2025-03-02', 'YYYY-MM-DD');

-- Standardize gender values
SELECT DISTINCT
    gen,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'Unknown'
    END AS standardized_gen
FROM 
    bronze.erp_cust_az12;

-- ============================================================================
-- >> ERP Location Table (erp_loc_a101)
-- ============================================================================

-- Check data standardization and consistency
SELECT
    cid,
    REPLACE(cid, '-', '') AS cleaned_cid,
    cntry
FROM
    bronze.erp_loc_a101
WHERE
    REPLACE(cid, '-', '') NOT IN (
        SELECT CST_KEY FROM silver.crm_cust_info
    );

-- Standardize country names
SELECT DISTINCT
    cntry,
    CASE
        WHEN UPPER(TRIM(cntry)) IN ('USA', 'US', 'UNITED STATUS') THEN 'United States'
        WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
        WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
        ELSE cntry
    END AS standardized_cntry
FROM
    bronze.erp_loc_a101;

-- ============================================================================
-- >> ERP Product Category Table (erp_px_cat_g1v2)
-- ============================================================================

-- Check for unwanted spaces in columns
SELECT DISTINCT
    cat,
    subcat,
    maintenance
FROM
    bronze.erp_px_cat_g1v2
WHERE
    cat != TRIM(cat) 
    OR subcat != TRIM(subcat) 
    OR maintenance != TRIM(maintenance);

-- Retrieve distinct values for category-related fields
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;

-- ============================================================================
