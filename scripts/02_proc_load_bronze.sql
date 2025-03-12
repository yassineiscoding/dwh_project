COPY bronze.crm_cust_info
FROM '/var/lib/postgresql/datasets/source_crm/cust_info.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.crm_prd_info
FROM '/var/lib/postgresql/datasets/source_crm/prd_info.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.crm_sales_details
FROM '/var/lib/postgresql/datasets/source_crm/sales_details.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_cust_az12
FROM '/var/lib/postgresql/datasets/source_erp/CUST_AZ12.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_loc_a101
FROM '/var/lib/postgresql/datasets/source_erp/LOC_A101.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_px_cat_g1v2
FROM '/var/lib/postgresql/datasets/source_erp/PX_CAT_G1V2.csv'
DELIMITER ','
CSV HEADER;
