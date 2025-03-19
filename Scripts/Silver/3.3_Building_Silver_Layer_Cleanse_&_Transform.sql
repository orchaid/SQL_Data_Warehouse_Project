/*
Description:
    This script is responsible for cleaning and transforming the data from the bronze layer and inserting it into the silver layer
    and it was written in tandum with [3.2_Checking_the_ETL_Problems] as they complete each other.

The cleaning and transformation process includes:
    - removing unwanted spaces
    - checking for duplicates and nulls
    - data validation for date columns
    - standardizing and normalizing data and ensuring data consistency.

Method Followed:
    - The cleaning and transformation process is done as I go one by one then will be put in a single query that holds all the transformations.
    - The result of this query is then copied into the silver layer equivalent table.
    - The quality checks are done after the transformation and before the insertion into the silver layer.
 */



--===========================================================================
TRUNCATE TABLE Silver.crm_cust_info RESTART IDENTITY;
-- Inserting into the silver layer (the final query for the cleaning and transformation of this table)
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
    TRIM (cst_firstname) AS cst_firstname, -- data cleansing
    TRIM (cst_lastname) cst_lastname,
    CASE WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
         WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
         ELSE 'Unknown' 
    END AS cst_marital_status, -- Normalize marital status values to readable format
    CASE WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
         WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
         ELSE 'Unknown' 
    END AS cst_gndr, -- Normalize gender values to readable format
    cst_create_date
FROM (
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag
FROM
    bronze.crm_cust_info
) -- data cleansing and filtering
WHERE flag = 1;
--===========================================================================
-- Check the result
SELECT * FROM silver.crm_cust_info LIMIT 1000;

------------------------
-- Quality checks

-- duplicates
SELECT 
    cst_id,
    Count(*) as count
FROM
    silver.crm_cust_info
GROUP BY cst_id
HAVING Count(*) > 1
ORDER BY cst_id DESC;

-- spaces
SELECT 
    cst_firstname
From 
    silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data standardization & consistency
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;
---no duplicates or nulls in pk, no unwanted spaces (done)
---------------------------

--===========================================================================
-- I have to make adjustments to the silver.crm_prd_info table because I changed the datatypes and add new column 
-- I will drop the table and recreate it
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
    prd_id INT
    ,cat_id VARCHAR(50)
    ,prd_key VARCHAR(50)
    ,prd_nm VARCHAR(50)
    ,prd_cost INT
    ,prd_line VARCHAR(50)
    ,prd_start_dt DATE
    ,prd_end_dt DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-------------------------------------------------------
TRUNCATE TABLE silver.crm_prd_info RESTART IDENTITY;
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
    REPLACE(SUBSTRING (prd_key,1 , 5), '-', '_') AS cat_id, -- to be similar to the id in erp_px_cat_g1v2  (derived column)
    SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost, -- replace nulls with 0 (data cleansing)
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'Unknown'
    END AS prd_line, -- (Normalization) 
    CAST(prd_start_dt AS DATE) AS prd_start_dt, -- data transformation
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE)-1 AS prd_end_dt -- adding new relevant dara (data enrichment)
FROM 
    bronze.crm_prd_info;
--===========================================================================

---------------------------
-- Quality checks

-- Check for Nulls or duplicates in the primary key
SELECT 
    prd_id,
    COUNT(prd_id) as count
FROM
    silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT (prd_id) > 1
ORDER BY prd_id DESC;


-- check for unwanted spaces
SELECT
    prd_key
FROM
    silver.crm_prd_info
WHERE prd_key != TRIM(prd_key);

-- checking for negative values or nulls in the cost 
SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- standardizing & consistency
SELECT DISTINCT prd_line FROM silver.crm_prd_info; 

-- Check for Invalid date orders
SELECT 
    prd_start_dt,
    prd_end_dt
FROM
    silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT * FROM silver.crm_prd_info LIMIT 1000;
----------------------------



--===========================================================================
-- I have to make adjustments to the silver.crm_sales_details table because I changed the datatypes and add new column
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num VARCHAR(50)
    ,sls_prd_key VARCHAR(50)
    ,sls_cust_id INT
    ,sls_order_dt DATE
    ,sls_ship_dt DATE
    ,sls_due_dt DATE
    ,sls_sales INT
    ,sls_quantity INT
    ,sls_price INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
----------------------------------------------------------
TRUNCATE TABLE silver.crm_sales_details RESTART IDENTITY;
-- Inserting into the silver layer (the final query for the cleaning and transformation of this table)
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
        WHEN sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0) -- if it's a zero make it null to avoid division by zero
        ELSE sls_price
    END AS sls_price
FROM
    bronze.crm_sales_details
--===========================================================================

---------------------------
-- Quality checks

-- check chronological order
SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt; -- (none)

-- check for negative values or nulls in the sales, quantity, price and does the sales = quantity * price?
SELECT DISTINCT
    sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE 
    sls_sales != sls_quantity * sls_price
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price; -- (none)

---------------------------





--===========================================================================
TRUNCATE TABLE silver.erp_cust_az12 RESTART IDENTITY;
-- No need to make adjustments to the silver.erp_cust_az12 table because I didn't change the datatypes or add new columns
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
--===========================================================================


---------------------------
-- Quality checks
SELECT * FROM silver.erp_cust_az12 LIMIT 1000;
-- checking for invalid dates
SELECT bdate
FROM
    silver.erp_cust_az12
WHERE bdate < '1925-1-1' OR bdate > CURRENT_DATE;

-- standardizing & consistency
SELECT DISTINCT gen FROM silver.erp_cust_az12;
---------------------------


--===========================================================================
TRUNCATE TABLE silver.erp_loc_a101 RESTART IDENTITY;
-- Loading the data into the silver layer
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT 
    REPLACE(cid,'-', '') AS cid,
    CASE WHEN TRIM(cntry) IN ('USA', 'US', 'United States') THEN 'United States'
         WHEN TRIM(cntry) IN ('DE', 'Germany') THEN 'Germany'
         WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unkown'
         ELSE TRIM(cntry) -- Normalize country names and replace nulls and blanks with 'Unknown'
    END AS cntry
FROM bronze.erp_loc_a101
--===========================================================================

---------------------------
-- Quality checks
SELECT * FROM silver.erp_loc_a101 LIMIT 1000;

SELECT DISTINCT cntry FROM silver.erp_loc_a101; 
---------------------------



--===========================================================================
-- No need to make adjustments to the silver.erp_px_cat_g1v2 table
RAISE NOTICE '>> Truncating Table:  silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2 RESTART IDENTITY;
RAISE NOTICE '>> Inserting Data Into:  silver.erp_px_cat_g1v2'
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
--===========================================================================
SELECT * FROM silver.erp_px_cat_g1v2 LIMIT 1000;
---------------------------