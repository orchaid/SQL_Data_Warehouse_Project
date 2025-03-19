/*
 Description: This script creates the bronze layer tables in the first half of the page
  and loads data into them from the source files (Source CRM and Source ERP -> Bronze) in the second half of the page.
*/

-- Create the bronze layer tables
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
    cst_id INT
    ,cst_key VARCHAR(50)
    ,cst_firstname VARCHAR(50)
    ,cst_lastname VARCHAR(50)
    ,cst_marital_status VARCHAR(50)
    ,cst_gndr    VARCHAR(50)
    ,cst_create_date DATE
);
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
    prd_id INT
    ,prd_key VARCHAR(50)
    ,prd_nm VARCHAR(50)
    ,prd_cost INT
    ,prd_line VARCHAR(50)
    ,prd_start_dt TIMESTAMP
    ,prd_end_dt TIMESTAMP
);
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details
(
    sls_ord_num VARCHAR(50)
    ,sls_prd_key VARCHAR(50)
    ,sls_cust_id INT
    ,sls_order_dt INT
    ,sls_ship_dt INT
    ,sls_due_dt INT
    ,sls_sales INT
    ,sls_quantity INT
    ,sls_price INT
);
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12
(
    CID	VARCHAR(50)
    ,BDATE DATE
    ,GEN VARCHAR(50)
);
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101
(
    CID VARCHAR(50)
    ,CNTRY VARCHAR(50)
);
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2
(
    ID VARCHAR(50)
    ,CAT VARCHAR(50)
    ,SUBCAT VARCHAR(50)
    ,MAINTENANCE VARCHAR(50)
);

/*
Truncate the table to remove existing data in case I run the script multiple times
or if I want to refresh the data in the table
Bulk insert data into bronze layer tables

============================================
Stored Procedure: load_bronze (Data Ingestion)
============================================

Usage:
    CALL bronze.load_bronze();
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE PLPGSQL
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '====================================================';
    RAISE NOTICE '🚀 Starting Data Load into Bronze Layer Tables...';
    RAISE NOTICE '====================================================';

    RAISE NOTICE '📌 Loading CRM Tables...';
    
    RAISE NOTICE '>> -------------------------------------';
    -- Loading bronze.crm_cust_info
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info RESTART IDENTITY;

    COPY bronze.crm_cust_info
    FROM 'D:/datasets/SQL Advanced/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH
        (
        DELIMITER ',',
        HEADER true,
        FORMAT csv
        );

    end_time := clock_timestamp();
    RAISE NOTICE '✅ Completed in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- Loading bronze.crm_prd_info
    RAISE NOTICE '>> -------------------';
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info RESTART IDENTITY;

    COPY bronze.crm_prd_info
    FROM 'D:/datasets/SQL Advanced/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (
        FORMAT csv, DELIMITER ',', HEADER true
    );
    end_time := clock_timestamp();
    RAISE NOTICE '✅ Completed in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    -- Loading bronze.crm_sales_details
    RAISE NOTICE '>> -------------------';
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details RESTART IDENTITY;

    COPY bronze.crm_sales_details
    FROM 'D:/datasets/SQL Advanced/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (
        FORMAT csv, DELIMITER ',', HEADER true
    );
    end_time := clock_timestamp();
    RAISE NOTICE '✅ Completed in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '📌 Loading ERP Tables...';

    -- Loading bronze.erp_px_cat_g1v2
    RAISE NOTICE '>> -------------------------------------';
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2 RESTART IDENTITY;

    COPY bronze.erp_px_cat_g1v2
    FROM 'D:/datasets/SQL Advanced/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT csv, DELIMITER ',', HEADER true
    );
    end_time := clock_timestamp();
    RAISE NOTICE '✅ Completed in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- Loading bronze.erp_cust_az12
    RAISE NOTICE '>> -------------------';
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12 RESTART IDENTITY;

    COPY bronze.erp_cust_az12
    FROM 'D:/datasets/SQL Advanced/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT csv, DELIMITER ',', HEADER true
    );
    end_time := clock_timestamp();
    RAISE NOTICE '✅ Completed in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- Loading bronze.erp_loc_a101
    RAISE NOTICE '>> -------------------';
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101 RESTART IDENTITY;

    COPY bronze.erp_loc_a101
    FROM 'D:/datasets/SQL Advanced/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (
        FORMAT csv, DELIMITER ',', HEADER true
    );
    end_time := clock_timestamp();
    RAISE NOTICE '✅ Completed in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '====================================================';
    RAISE NOTICE '🎉 Data Load Completed Successfully!';
    RAISE NOTICE '====================================================';
END
$$;

-- to call the procedure
CALL bronze.load_bronze();


