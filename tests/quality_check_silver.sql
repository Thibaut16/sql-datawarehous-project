/*
===========================================================================================================================================================
Quality Checks
===========================================================================================================================================================
Script Purpose:
   This script performs various quality checks for data consistency, accuracy, and
   standardization across the 'silver' schema. It includes checks for:
   -Null or duplicate primary keys
   -Unwanted spaces in string fields
   -Data standardization and consistency
   -Invalid date ranges and orders
   -Data consistency between related fields

Usage Notes:
  - Run these checks after data loading Silver layer.
  - Investigate and resolve any discrepancies found during cheks.
*/
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Quality Checks table silver.crm_cust_info
--rerun quality checks silver layer table silver.crm_cust_info
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

--check for Nulls or duplicates in primary key
--Expectation: No Result
Select 
cst_id,
COUNT(*)
From silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1  or cst_id IS NULL --id is duplicated OR are there records where primary key null ist

GO

--silver solve duplicates problem
Select 
*
FROM silver.crm_cust_info
WHERE cst_id=29466   

GO

--cst_id=29466 appears 3 times so we search for date rank it according to latest to oldest and select latest
Select 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date Desc) as flag_last
FROM silver.crm_cust_info
WHERE cst_id=29466  

GO
--2-check for unwanted spaces in string values e,g cst_firstname
--Expectation: No Results (we dont want spaces)
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname!= TRIM(cst_firstname)

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Quality Checks table silver.crm_prd_info
--rerun quality checks silver layer table silver.crm_prd_info
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--check for Nulls or duplicates in primary key
--Expectation: No Result
Select 
prd_id,
COUNT(*)
From silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1  or prd_id IS NULL --id is duplicated OR are there records where primary key null ist

GO

--unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm!= TRIM(prd_nm)

GO
--check for cost it shouldnt be negative or null
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost is Null   

GO

--data standadization
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

GO

--6-check for INVALID DATES end smaller than start
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

GO
SELECT *
FROM silver.crm_prd_info
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Quality Checks table silver.crm_sales_details
--rerun quality checks silver layer table silver.crm_sales_details
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--invalid dates
select *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt --check if there are order dates greater than due or ship date

--- data consistency Business rule : sales= price *quantity
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price

FROM silver.crm_sales_details
WHERE sls_sales != sls_price*sls_quantity
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales < =0 OR sls_quantity < =0 OR sls_price < =0
ORDER BY sls_sales, sls_quantity, sls_price

--final look
SELECT * FROM silver.crm_sales_details
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Quality Checks table silver.erp_cust_az12
--rerun quality checks silver layer table silver.erp_cust_az12
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

--identify out of range dates
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate  < '1924-01-01'  OR bdate > GETDATE()

--data standardisation and consistency
SELECT DISTINCT 
gen 
FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Quality Checks table silver.erp_loc_a101
--rerun quality checks silver layer table silver.erp_loc_a101
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--data standardization and consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Quality Checks table silver.erp_px_cat_g1v2
--rerun quality checks silver layer table silver.erp_px_cat_g1v2
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 --check for unwanted spaces
SELECT id,cat,subcat,maintenance FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)  

--Data standardization & consistency
SELECT DISTINCT 
--id,
cat,
subcat,
maintenance 
FROM silver.erp_px_cat_g1v2
