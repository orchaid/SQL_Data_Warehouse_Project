

--------------------(integration) gender for dim.customer--------------------

/*
After I joined the data correctly I checked if there ware duplicates in the data. (there were none)

There is two gender columns in the data, so I am gonna do data integration 
    I asked and the master source of customer data is the crm
*/


SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
FROM
    silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
WHERE cst_gndr <> gen 
    AND (cst_gndr <> 'Unknown' OR gen <> 'Unknown') -- the issue is the unknown values and small contradictions
ORDER BY 1,2


-- integrating the two system in one at the case statment for gender
SELECT
    ci.cst_gndr,
    ca.gen,
    CASE 
        WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- crm is the master for gender info
        WHEN ci.cst_gndr = 'Unknown' THEN ca.gen
    END gender
FROM
    silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
WHERE cst_gndr <> gen 
    AND (cst_gndr = 'Unknown' OR gen = 'Unknown')


--------------------(Integrating) for dim.products-------------------


-- contains historical information and current information specified at the start and end date
-- the question is whether or not I should include the historical information in my view or narrow it down for the current info only



SELECT prd_key , COUNT(*)FROM ( -- Checking the count of prd_key in order to join this with the sales
SELECT 
    pi.prd_id,
    pi.cat_id,
    pi.prd_key,
    pi.prd_nm,
    pi.prd_cost,
    pi.prd_line,
    pi.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
FROM
    silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pc.id = pi.cat_id
WHERE pi.prd_end_dt IS NULL -- Filter out all historical data

)
GROUP BY prd_key
HAVING COUNT(*) > 1


--------------------(Integrating) for fact_sales-------------------


-- many to one relationship when I connect this to the other 2 view
SELECT 
    
    sls_prd_key,
    COUNT(*)
FROM
    silver.crm_sales_details sd
GROUP BY sls_prd_key
HAVING COUNT(*) > 1



-- I am connecting my data model based on the surrogate keys not the keys from the source system
-- We use the dimension surrogate keys instead of IDs to easily connect facts with dimensions
/* So I will join this query with the two dimension views I created before 
    to select (use) their keys (row_number()) in here */
-- data look up: joining the table to get one information


CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num AS order_number,
    dp.product_key,
    dc.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS  sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM
    silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc
    ON sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp
    ON sd.sls_prd_key = dp.product_number