/*
=======================================================================
Create Database and Schemas
=======================================================================
Script Purpose:
   This script creates a new database named  'DataWarehouse' after checking if it already exists.
   If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
   within the database: 'bronze', 'silver', 'gold'.

WARNING:
    Running this script will drop the entire  'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.

*/
--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
  BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
  END;
GO

use master;

--create database name it DatabaseWarehouse
create database DatabaseWarehouse;
GO
USE DatabaseWarehouse;

--create schemas here in our project 3 bronze, silver, gold ; to find go to db-security(sicherheit)-schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO   --its a batch seperator in sql to run to commands at a time
CREATE SCHEMA gold;

--Create table in bronze layer
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_cust_info;
CREATE Table bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_prd_info;
CREATE Table bronze.crm_prd_info (
 prd_id INT,
 prd_key NVARCHAR(50),
 prd_nm NVARCHAR(50),
 prd_cost INT,
 prd_line NVARCHAR(50),
 prd_start_dt DATETIME,
 prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE bronze.crm_sales_details;
CREATE Table bronze.crm_sales_details (
 sls_ord_num NVARCHAR(50),
 sls_prd_key NVARCHAR(50),
 sls_cust_id INT,
 sls_order_dt INT,
 sls_ship_dt INT,
 sls_due_dt INT,
 sls_sales INT,
 sls_quantity INT,
 sls_price INT
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE bronze.erp_loc_a101;
CREATE Table bronze.erp_loc_a101 (
 cid NVARCHAR(50),
 cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE bronze.erp_cust_az12;
CREATE Table bronze.erp_cust_az12 (
 cid NVARCHAR(50),
 bdate DATE,
 gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE bronze.erp_px_cat_g1v2;
CREATE Table bronze.erp_px_cat_g1v2 (
 id NVARCHAR(50),
 cat NVARCHAR(50),
 subcat NVARCHAR(50),
 maintenance NVARCHAR(50)
);

--to insert data in to table first make it empty to avoid duplicates
TRUNCATE TABLE bronze.crm_cust_info;

--insert DATA per 1 load from source in to table
BULK INSERT bronze.crm_cust_info;
FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  TABLOCK
);

--check source from new table
SELECT * FROM bronze.crm_cust_info
--count rows
SELECT count(*) FROM bronze.crm_cust_info

--EXEC sp_rename 'bronze.crm_prod_info','bronze.crm_prd_info';

--to insert data in to table first make it empty to avoid duplicates
TRUNCATE TABLE bronze.crm_prd_info;
--insert DATA per 1 load from source in to table
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  TABLOCK
);

--check source from new table
SELECT * FROM bronze.crm_prd_info
--count rows
SELECT count(*) FROM bronze.crm_cust_info


--to insert data in to table first make it empty to avoid duplicates
TRUNCATE TABLE bronze.crm_sales_details;
--insert DATA per 1 load from source in to table
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  TABLOCK
);

--check source from new table
SELECT * FROM bronze.crm_sales_details
--count rows
SELECT count(*) FROM bronze.crm_sales_details


--to insert data in to table first make it empty to avoid duplicates
TRUNCATE TABLE bronze.erp_cust_az12;
--insert DATA per 1 load from source in to table
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  TABLOCK
);

--check source from new table
SELECT * FROM bronze.erp_cust_az12
--count rows
SELECT count(*) FROM bronze.erp_cust_az12


--to insert data in to table first make it empty to avoid duplicates
TRUNCATE TABLE bronze.erp_loc_a101;
--insert DATA per 1 load from source in to table
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  TABLOCK
);

--check source from new table
SELECT * FROM bronze.erp_loc_a101
--count rows
SELECT count(*) FROM bronze.erp_loc_a101

--to insert data in to table first make it empty to avoid duplicates
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
--insert DATA per 1 load from source in to table
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  TABLOCK
);

--check source from new table
SELECT * FROM bronze.erp_px_cat_g1v2
--count rows
SELECT count(*) FROM bronze.erp_px_cat_g1v2
