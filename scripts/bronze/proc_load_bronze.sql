/*
===============================================================================
Stored Procedure: Load Bronze Layer 
===============================================================================
COPY Error:
  PostgreSQL looks for the CSV file inside the database server's file sytem,
  not on the personal computer's folder.

Solution:
  Move the CSV file to the PostgreSQL directory.

Directory: 
  'C:\Program Files\PostgreSQL\17\data\'
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration INTERVAL;
BEGIN
	------------------------------------------------------------------------------
	-- Load CRM Data
	------------------------------------------------------------------------------
	RAISE NOTICE 'Loading CRM data...';
	start_time := clock_timestamp();
	
	TRUNCATE TABLE bronze.crm_cust_info;
	COPY bronze.crm_cust_info (
		cst_id, cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gender,
		cst_create_date
	)
	FROM 'your_filepath'
	DELIMITER ','
	CSV HEADER
	;
	
	TRUNCATE TABLE bronze.crm_prd_info;
	COPY bronze.crm_prd_info (
		prd_id, prd_key,
		prd_nm, prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	FROM 'your_filepath'
	DELIMITER ','
	CSV HEADER
	;
	
	TRUNCATE TABLE bronze.crm_sales_details;
	COPY bronze.crm_sales_details (
		sls_ord_num, sls_prd_key,
		sls_cust_id, sls_order_dt,
		sls_ship_dt, sls_due_dt,
		sls_sales, sls_quantity,
		sls_price
	)
	FROM 'your_filepath'
	DELIMITER ','
	CSV HEADER
	;

	end_time := clock_timestamp();
	duration := end_time - start_time;

	RAISE NOTICE 'CRM data loaded in % seconds', EXTRACT(EPOCH FROM duration);

	------------------------------------------------------------------------------
	-- Load CRM Data
	------------------------------------------------------------------------------
	RAISE NOTICE 'Loading ERP data...';
	start_time := clock_timestamp();
	
	TRUNCATE TABLE bronze.erp_cust_az12;
	COPY bronze.erp_cust_az12 (cid, bdate, gen)
	FROM 'your_filepath'
	DELIMITER ','
	CSV HEADER
	;
	
	TRUNCATE TABLE bronze.erp_loc_a101;
	COPY bronze.erp_loc_a101 (cid, cntry)
	FROM 'your_filepath'
	DELIMITER ','
	CSV HEADER
	;
	
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	FROM 'your_filepath'
	DELIMITER ','
	CSV HEADER
	;

	end_time := clock_timestamp();
	duration := end_time - start_time;

	RAISE NOTICE 'ERP data loaded in % seconds', EXTRACT(EPOCH FROM duration);
	
	RAISE NOTICE 'Loading data executed successfully!';

EXCEPTION
	WHEN unique_violation THEN
		RAISE NOTICE 'Duplicate key - record already exists.';
	WHEN foreign_key_violation THEN
		RAISE NOTICE 'Foreign key constraint failed.';
	WHEN OTHERS THEN
		RAISE NOTICE 'Error occured: %', SQLERRM;

END;
$$;
