
/* 
I will make this whole process into a stored procedure to make it easier to run the script
and to make it more organized

==================================================================================================================
Stored Procedure to Load Data into Silver Layer Tables
==================================================================================================================
Script Purpose:
This stored procedure preforms the ETL (Extract, Transform, Load) process from the Bronze Layer Tables into the Silver Layer Tables.
    Actions:
        - Truncate the Silver Layer Tables
        - Insert cleaned & trasformed data from the Bronze Layer Tables into the Silver Layer Tables
    Usage:
        - Call the stored procedure to load the data into the Silver Layer Tables 
        CALL silver.load_silver();
    
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE PLPGSQL
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '====================================================';
    RAISE NOTICE '🚀 Starting Data Load into Bronze Layer Tables...';
    RAISE NOTICE '====================================================';

    RAISE NOTICE '>> -------------------------------------';
    RAISE NOTICE '📌 Loading CRM Tables...';
    RAISE NOTICE '>> -------------------------------------';
    RAISE NOTICE '>> Truncating Table:  silver.crm_cust_info';
    TRUNCATE TABLE Silver.crm_cust_info RESTART IDENTITY;
    start_time := clock_timestamp();

    RAISE NOTICE '>> Inserting Data Into:  silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM (cst_firstname) AS cst_firstname, 
        TRIM (cst_lastname) cst_lastname,
        CASE WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
            WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
            ELSE 'Unknown' 
        END AS cst_marital_status,
        CASE WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
            WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
            ELSE 'Unknown' 
        END AS cst_gndr, 
        cst_create_date
    FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag
    FROM
        bronze.crm_cust_info
    )
    WHERE flag = 1;

    end_time := clock_timestamp();
    RAISE NOTICE '✅ Duration for crm_cust_info: %', end_time - start_time;

    RAISE NOTICE '>> Truncating Table:  silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info RESTART IDENTITY;
    start_time := clock_timestamp();

    RAISE NOTICE '>> Inserting Data Into:  silver.crm_prd_info';
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
        REPLACE(SUBSTRING (prd_key,1 , 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
        TRIM(prd_nm) AS prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost, 
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'Unknown'
        END AS prd_line, 
        CAST(prd_start_dt AS DATE) AS prd_start_dt, 
        CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE)-1 AS prd_end_dt 
    FROM bronze.crm_prd_info;

    end_time := clock_timestamp();
    RAISE NOTICE '✅ Duration for crm_prd_info: %', end_time - start_time;

    RAISE NOTICE '>> Truncating Table:  silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details RESTART IDENTITY;
    start_time := clock_timestamp();
    RAISE NOTICE '>> Inserting Data Into:  silver.crm_sales_details';
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
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales!= sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price < 0 THEN ABS(sls_price)
            WHEN sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0) 
            ELSE sls_price
        END AS sls_price
    FROM
        bronze.crm_sales_details;

    end_time := clock_timestamp();
    RAISE NOTICE '✅ Duration for crm_sales_details: %', end_time - start_time;
    
    RAISE NOTICE '>> -------------------------------------';
    RAISE NOTICE '📌 Loading ERP Tables...';
    RAISE NOTICE '>> -------------------------------------';

    RAISE NOTICE '>> Truncating Table:  silver.erb_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12 RESTART IDENTITY;
    start_time := clock_timestamp();
    RAISE NOTICE '>> Inserting Data Into:  silver.erb_cust_az12';
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE WHEN bdate < '1925-1-1' OR bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(gen)) IS NULL THEN 'Unknown'
                WHEN UPPER(TRIM(gen)) = '' THEN 'Unknown'
                ELSE gen
        END AS gen_new
    FROM bronze.erp_cust_az12;

    end_time := clock_timestamp();
    RAISE NOTICE '✅ Duration for erp_loc_a101: %', end_time - start_time;

    RAISE NOTICE '>> Truncating Table:  silver.erb_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101 RESTART IDENTITY;
    start_time := clock_timestamp();
    -- Loading the data into the silver layer
    RAISE NOTICE '>> Inserting Data Into:  silver.erb_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT 
        REPLACE(cid,'-', '') AS cid,
        CASE WHEN TRIM(cntry) IN ('USA', 'US', 'United States') THEN 'United States'
            WHEN TRIM(cntry) IN ('DE', 'Germany') THEN 'Germany'
            WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unkown'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    end_time := clock_timestamp();
    RAISE NOTICE '✅ Duration for erp_px_cat_g1v2: %', end_time - start_time;

    RAISE NOTICE '>> Truncating Table:  silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2 RESTART IDENTITY;
    start_time := clock_timestamp();
    RAISE NOTICE '>> Inserting Data Into:  silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        TRIM(cat) AS cat,
        TRIM(subcat) AS subcat,
        TRIM(maintenance) AS maintenance
    FROM
        bronze.erp_px_cat_g1v2;
    end_time := clock_timestamp();
    RAISE NOTICE '✅ Duration for erp_px_cat_g1v2: %', end_time - start_time;

    RAISE NOTICE '====================================================';
    RAISE NOTICE '🎉 Data Load Completed Successfully!';
    RAISE NOTICE '====================================================';
END $$;

CALL silver.load_silver();