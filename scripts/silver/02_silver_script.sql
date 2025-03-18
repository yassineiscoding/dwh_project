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

SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    dwh_create_date
FROM bronze.crm_prd_info