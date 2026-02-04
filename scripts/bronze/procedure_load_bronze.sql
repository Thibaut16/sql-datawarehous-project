/*
==========================================================================================================
Store Procedure: Load Bronze Layer (SOURCE -> Bronze)
==========================================================================================================
Script purpose:
  This stored procedure loads data into the bronze schema from external csv files.
  It performs the following actions:
   -Truncates the bronze tables before loading the data
   -Uses the 'BULK INSERT' command to load data from the csv files to bronze tables
   -It also tracks execution time and does some printing for debugging.
Parameters:
  None
  This stored procedure does not accept any parameters nor returns any value
Usage example:
EXEC bronze.load_bronze;
*/ 

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
  BEGIN TRY
    SET @batch_start_time = GETDATE();
    PRINT'===============================';
	PRINT'Loading Bronze Layer';
	PRINT'===============================';

	PRINT'--------------------------------';
	PRINT'Loading CRM Tables';
	PRINT'--------------------------------';

	SET @start_time = GETDATE();
	PRINT'>>>TRUNCATING: bronze.crm_cust_info';
	--to insert data in to table first make it empty to avoid duplicates
	TRUNCATE TABLE bronze.crm_cust_info;
	--insert DATA per 1 load from source in to table
	PRINT'>>>Inserting Data into: bronze.crm_cust_info';

	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>>>>load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	--check source from new table
	SELECT * FROM bronze.crm_cust_info
	--count rows
	SELECT count(*) FROM bronze.crm_cust_info

	--EXEC sp_rename 'bronze.crm_prod_info','bronze.crm_prd_info';
	SET @start_time = GETDATE();
	PRINT'>>>TRUNCATING: bronze.crm_prd_info';
	--to insert data in to table first make it empty to avoid duplicates
	TRUNCATE TABLE bronze.crm_prd_info;
	--insert DATA per 1 load from source in to table
	PRINT'>>>Inserting Data into: bronze.crm_cust_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>>>>load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	--check source from new table
	SELECT * FROM bronze.crm_prd_info
	--count rows
	SELECT count(*) FROM bronze.crm_cust_info

	SET @start_time = GETDATE();
	PRINT'>>>TRUNCATING: bronze.crm_sales_details';
	--to insert data in to table first make it empty to avoid duplicates
	TRUNCATE TABLE bronze.crm_sales_details;
	--insert DATA per 1 load from source in to table
	PRINT'>>>Inserting Data into: bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>>>>load duration:' + CAST(DATEDIFF(second, @start_time , @end_time) AS NVARCHAR) + 'seconds';

	--check source from new table
	SELECT * FROM bronze.crm_sales_details;
	--count rows
	SELECT count(*) FROM bronze.crm_sales_details

	PRINT'--------------------------------';
	PRINT'Loading ERP Tables';
	PRINT'--------------------------------';
	--to insert data in to table first make it empty to avoid duplicates
	SET @start_time = GETDATE();
	PRINT'>>>TRUNCATING: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	--insert DATA per 1 load from source in to table
	PRINT'>>>Inserting Data into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>>>>load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	--check source from new table
	SELECT * FROM bronze.erp_cust_az12
	--count rows
	SELECT count(*) FROM bronze.erp_cust_az12


	--to insert data in to table first make it empty to avoid duplicates
	SET @start_time = GETDATE();
	PRINT'>>>TRUNCATING: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	--insert DATA per 1 load from source in to table
	PRINT'>>>Inserting Data into: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>>>>load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	--check source from new table
	SELECT * FROM bronze.erp_loc_a101
	--count rows
	SELECT count(*) FROM bronze.erp_loc_a101

	--to insert data in to table first make it empty to avoid duplicates
	SET @start_time = GETDATE();
	PRINT'>>>TRUNCATING: erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	--insert DATA per 1 load from source in to 
	PRINT'>>>Inserting Data into: bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\JORDIS Kegne\Downloads\sqldwh\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>>>>load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	--check source from new table
	SELECT * FROM bronze.erp_px_cat_g1v2
	--count rows
	SELECT count(*) FROM bronze.erp_px_cat_g1v2

	SET @batch_end_time= GETDATE();
	PRINT'===============================';
	PRINT'Loading Bronze Layer is completed';
	PRINT'- Total load duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	PRINT'===============================';
  END TRY
  BEGIN CATCH
    PRINT'=================================';
	PRINT'Error occured during loading bronze layer';
	PRINT'Error message'+ ERROR_MESSAGE();
	PRINT'Error message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'Error message'+ CAST(ERROR_STATE() AS NVARCHAR);
	PRINT'=================================';
  END CATCH
END
