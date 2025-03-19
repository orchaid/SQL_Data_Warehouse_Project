/*
==============================================================================
Quality Checks
==============================================================================

*/


--------------------(Validation) gender for dim.customer--------------------

SELECT *
FROM gold.dim_customers
WHERE gender IS NULL -- this is only one row and it has no informations in any column

--DROP VIEW gold.dim_customers

-- There was a null value because of a customer key  but I removed from the view 
SELECT DISTINCT gender FROM gold.dim_customers


--------------------(Validation) for dim.products-------------------

-- none was needed
SELECT * FROM gold.dim_products


--------------------(Validation) for fact_sales---------------------


-- check the joined views.. is it sucessful without nulls in the primary keys or not

/* The type of relationship here:
one mandatory to many optional: because there are many options in the sales table
	customers who hadn't bought
	customer who bought once
	customers who bought more than once*/


SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL


