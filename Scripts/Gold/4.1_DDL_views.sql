/*
==============================================================================
DDL Script: Create Gold Views
==============================================================================

Purpose:
    this script creates the views for the Gold Layer in the data warehouse from the silver layer to create (star schema).
    each view is created from a different table or different joined tables

Usage:
    can be quaried for creating views that are used for reporting

==============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

CREATE VIEW gold.dim_customers AS
SELECT * FROM 
(
SELECT 
    ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- crm is the master for gender info
        WHEN ci.cst_gndr = 'Unknown' THEN ca.gen
    END gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date
FROM
    silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
)
WHERE customer_number != 'A01Ass' -- this is one customer key that has no info in all the other customers so I can't do anything about it


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================


CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pi.prd_cost AS product_cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM
    silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pc.id = pi.cat_id
WHERE pi.prd_end_dt IS NULL -- Filter out all historical data



-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================



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
