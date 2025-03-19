-- Exploring the data in the silver layer 
-- and finding the relationships between the tables

SELECT * FROM silver.crm_cust_info LIMIT 10;

SELECT * FROM silver.crm_prd_info LIMIT 100;


SELECT *
FROM 
    silver.crm_sales_details
LIMIT 100;


SELECT * FROM silver.erp_cust_az12 LIMIT 10;

SELECT * FROM silver.erp_loc_a101 LIMIT 1000;

SELECT * FROM silver.erp_px_cat_g1v2 LIMIT 1000;


/* Creating the silver layer (DDL) by copying from the silver layer tables.

    I am adding an extra for the meta data about the creation time of the record.
*/

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info
(
    cst_id INT
    ,cst_key VARCHAR(50)
    ,cst_firstname VARCHAR(50)
    ,cst_lastname VARCHAR(50)
    ,cst_marital_status VARCHAR(50)
    ,cst_gndr    VARCHAR(50)
    ,cst_create_date DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
    prd_id INT
    ,prd_key VARCHAR(50)
    ,prd_nm VARCHAR(50)
    ,prd_cost INT
    ,prd_line VARCHAR(50)
    ,prd_start_dt TIMESTAMP
    ,prd_end_dt TIMESTAMP,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details
(
    sls_ord_num VARCHAR(50)
    ,sls_prd_key VARCHAR(50)
    ,sls_cust_id INT
    ,sls_order_dt INT
    ,sls_ship_dt INT
    ,sls_due_dt INT
    ,sls_sales INT
    ,sls_quantity INT
    ,sls_price INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12
(
    CID	VARCHAR(50)
    ,BDATE DATE
    ,GEN VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101
(
    CID VARCHAR(50)
    ,CNTRY VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2
(
    ID VARCHAR(50)
    ,CAT VARCHAR(50)
    ,SUBCAT VARCHAR(50)
    ,MAINTENANCE VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


