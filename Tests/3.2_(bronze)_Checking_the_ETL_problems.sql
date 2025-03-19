/*
Checking the ETL problems:
    - removing unwanted spaces
    - checking for duplicates and nulls
    - data validation for date columns
    - standardizing and normalizing data and ensuring data consistency.
*/

-------------- bronze.crm_cust_info --------------


SELECT * 
FROM bronze.crm_cust_info 

-- Check for Nulls or duplicates in the primary key
-- Expectation: no result (no nulls or duplicates)
SELECT 
    cst_id,
    Count(*) as count
FROM
    bronze.crm_cust_info
GROUP BY cst_id
HAVING Count(*) > 1
ORDER BY cst_id DESC;

-- Looking at the duplicates 
SELECT *
FROM
    bronze.crm_cust_info
WHERE cst_id = 29466;

-- keep the latest record and delete the rest

SELECT *
FROM (
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag
FROM
    bronze.crm_cust_info
-- WHERE cst_id = 29466;
)
WHERE flag = 1; -- flag = 1 is where is no duplicates


-- Check for unwanted spaces
-- Expectation: no result (no unwanted spaces)
SELECT 
    cst_firstname
From 
    bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data standardization & consistency
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

----- no duplicates or nulls in pk, no unwanted spaces
SELECT 
    cst_id,
    cst_key,
    TRIM (cst_firstname) AS cst_firstname,
    TRIM (cst_lastname) cst_lastname,
    CASE WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
         WHEN TRIM(cst_marital_status) = 'M' THEN 'Married',
         ELSE 'Unknown' 
    END AS cst_marital_status,
    CASE WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
         WHEN TRIM(cst_gndr) = 'F' THEN 'Female',
         ELSE 'Unknown' 
    END AS cst_gndr,
    cst_create_date
FROM (
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag
FROM
    bronze.crm_cust_info
-- WHERE cst_id = 29466;
)
WHERE flag = 1;

-------------- bronze.crm_prd_info --------------



-- this table has lots of primary keys
SELECT *
FROM
    bronze.crm_prd_info

-- Check for Nulls or duplicates in the primary key
SELECT 
    prd_id,
    COUNT(prd_id) as count
FROM
    bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT (prd_id) > 1
ORDER BY prd_id DESC;


-- check for unwanted spaces
SELECT
    prd_key
FROM
    bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key); --(none)


-- check for relation keys
SELECT id FROM bronze.erp_px_cat_g1v2;  -- the seperator is _ not -
SELECT sls_prd_key FROM bronze.crm_sales_details;  -- same key with more characters

-- check if the id in erp_px_cat_g1v2 is in the prd_key in crm_prd_info
SELECT id FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT REPLACE(SUBSTRING (prd_key,1 , 5), '-', '_') FROM bronze.crm_prd_info);

-- check if the prd_key in crm_prd_info is in the id in erp_px_cat_g1v2
SELECT sls_prd_key FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT SUBSTRING(prd_key,7,LENGTH(prd_key)) FROM bronze.crm_prd_info);
-- then tables have a relation between the crm_prd_info and the mentioned above tables


-- check for unwanted spaces
SELECT
    prd_nm
FROM
    bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); --(none)


-- checking for negative values or nulls in the cost 
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL; --(there is nulls) the business rule is to replace the nulls with 0

-- standardizing & consistency
SELECT DISTINCT prd_line FROM bronze.crm_prd_info; -- give the abbreviation names



-- Check for Invalid date orders
SELECT 
    prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt; -- this doesn't make sense so I will look at a subset of it in excel
-- even if I switch the start and end date, the other data is not consistent with the dates
-- I will create a new column from the start date column as the end date by LEAD function (after asking experts if it's okay)

SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_start_dt,
    LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)- INTERVAL '1 day' AS prd_end_dt_test -- interval is because the time stamp
FROM
    bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'CL-JE-LJ-0192-X');


-------------- bronze.crm_sales_details --------------



SELECT * FROM bronze.crm_sales_details;


-- Check for unwanted spaces in sls_ord_num , sls_prd_key
SELECT *
FROM
    bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num); -- (none)


SELECT *
FROM
    bronze.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key); -- (none)


-- check for relation keys
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info); -- (none) so all the keys are in the prd_key in crm_prd_info

SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info); -- (none) so all the keys are in the cst_id in crm_cust_info


-- Check for Nulls or duplicates in the primary key
SELECT
    sls_prd_key
FROM
    bronze.crm_sales_details
WHERE sls_prd_key IS NULL;

SELECT
    sls_cust_id
FROM
    bronze.crm_sales_details
WHERE sls_cust_id IS NULL;


-- check for invalid dates 
SELECT 
    sls_order_dt,
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
    OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 -- date format here YYYYMMDD so the length should be 8
    OR sls_order_dt > 20201230
    OR sls_order_dt < 20001230; -- upper and lower bounds


SELECT 
    sls_order_dt,
    NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
    OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8
    OR sls_ship_dt > 20201230
    OR sls_ship_dt < 20001230;


SELECT 
    sls_due_dt,
    NULLIF(sls_due_dt,0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8
    OR sls_due_dt > 20201230
    OR sls_due_dt < 20001230;

-- check chronological order
SELECT
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM
    bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt
    OR sls_ship_dt > sls_due_dt; -- (none)



-- check for negative values or nulls in the sales, quantity, price and does the sales = quantity * price?
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM
    bronze.crm_sales_details
WHERE 
    sls_sales != sls_quantity * sls_price
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;
/*
The data of these three coulmns have many issues (both the price and the sales have negative values and nulls, the sales is not equal to the quantity * price in few cases)
so after discussing with the business, 
    I will replace the negative Values in the price with its positive counterpart
    if the price is null, I will replace it with the average price of the product or calculate it from the sales and quantity
    if the sales is null or negative or zero, I will derive it from the quantity * price
*/
SELECT DISTINCT
    CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales!= sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price < 0 THEN ABS(sls_price)
         WHEN sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0) -- if it's a zero make it null to avoid division by zero
         ELSE sls_price
    END AS sls_price
FROM
    bronze.crm_sales_details
WHERE 
    sls_sales != sls_quantity * sls_price
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;


-------------- bronze.erp_cust_az12 --------------



SELECT * FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000'; -- the same key

SELECT cst_key FROM bronze.crm_cust_info; -- the same key



-- cst_key in crm_cust_info is the same as cid in erp_cust_az12 after removing the first 3 characters
SELECT 
    cid,
    bdate,
    gen,
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
        ELSE cid
    END AS cst_key
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
        ELSE cid END NOT IN (SELECT cst_key FROM bronze.crm_cust_info);


-- checking for invalid dates
SELECT bdate
FROM
    bronze.erp_cust_az12
WHERE bdate < '1930-1-1' OR bdate > CURRENT_DATE; -- the date that is not in the range would be removed

-- solution: replace the invalid dates with nulls
SELECT
    bdate,
    CASE WHEN bdate < '1925-1-1' OR bdate > CURRENT_DATE THEN NULL
         ELSE bdate
    END AS bdate
FROM
    bronze.erp_cust_az12
WHERE bdate < '1925-1-1' OR bdate > CURRENT_DATE;


-- standardizing & consistency
SELECT DISTINCT gen_new FROM(
SELECT DISTINCT gen,
    CASE WHEN TRIM(gen)= 'F' THEN 'Female'
             WHEN TRIM(gen)= 'M' THEN 'Male'
             WHEN TRIM(gen) IS NULL THEN 'Unknown'
             WHEN TRIM(gen)= '' THEN 'Unknown'
             ELSE gen
    END AS gen_new
FROM bronze.erp_cust_az12);
-- OR
SELECT DISTINCT gen,
    CASE WHEN TRIM(gen) IN ('F', 'Female') THEN 'Female'
             WHEN TRIM(gen) IN ('M', 'Male') THEN 'Male'
             ELSE 'Unknown'
    END AS gen_new
FROM bronze.erp_cust_az12;



-------------- bronze.crm_loc_a101 --------------



SELECT * FROM bronze.erp_loc_a101;


-- same key in the two tables
SELECT 
    cid,
    REPLACE(cid,'-', '') AS cid_new
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-', '') NOT IN (SELECT cid FROM silver.erp_cust_az12);

-- check for unwanted spaces
SELECT cntry FROM bronze.erp_loc_a101
WHERE cntry != TRIM(cntry); -- (no unwanted spaces but there are blanks)

SELECT DISTINCT cntry FROM bronze.erp_loc_a101; -- need to standardize the country names

-- standardizing & consistency
SELECT DISTINCT cntry FROM (
SELECT 
    CASE WHEN TRIM(cntry) IN ('USA', 'US', 'United States') THEN 'United States'
         WHEN TRIM(cntry) IN ('DE', 'Germany') THEN 'Germany'
         WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unkown'
         ELSE TRIM(cntry)
    END AS cntry
FROM
    bronze.erp_loc_a101)

-------------- bronze.erp_px_cat_g1v2 --------------


SELECT * FROM bronze.erp_px_cat_g1v2;

-- check the relation keys
SELECT 
    id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info); -- only one key is not in the prd_key in crm_prd_info and I will leave it at that

-- check for unwanted spaces
SELECT cat FROM bronze.erp_px_cat_g1v2 WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance); -- (none)

-- standardizing & consistency
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2; -- (Normalized already)

SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2 ORDER BY 1; -- (Normalized already)

