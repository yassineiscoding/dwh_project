-- Inserting into silver.crm_cust_info
INSERT INTO silver.crm_cust_info(
	cst_id ,
    cst_key,
    cst_firstname ,
    cst_lastname ,
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
cst_id ,
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

-- Inserting into silver.crm_prd_info

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

-- Inserting into silver.crm_sales_details

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

-- Inserting into silver.erp_cust_az12

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
FROM bronze.erp_cust_az12

-- Inserting into silver.erp_loc_a101

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
FROM
    bronze.erp_loc_a101;

-- Inserting into silver.erp_px_cat_g1v2
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

