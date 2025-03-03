/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


-- What should be done?
--		1. Saving frequently used code in STORED PROCEDURES in database
--		(In our case, we will use the FULL LOAD of the data every day
--		and run this script to overwrite all the rows.)
--		2. Adding PRINTS to track execution, debug issues, and understand the flow
--		3. Adding TRY...CATCH to handle error handling, data integrity, and issue logging for easier debugging
--		4. Track the ETL duration to identify the bottlenecks, optimize performance, detect issues
--		(For that declare datetime variables, call them before each table insert and track the execution time.
--		 Add as well the duration of loading whole "BRONZE" batch layer)

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE(); 
		PRINT '=================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';
		-- Truncating the table, so that the rows are not loaded twice after running the script

		SET @start_time = GETDATE();
		PRINT '>> Truncating TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Pavlo\Portfolio-Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- Actual data starts from the second row, the first is the column names
			FIELDTERMINATOR = ',', -- Delimeter between the fields
			TABLOCK -- Locks the whole table during the insertion
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		-- Check
		--		1. if the values in the columns correspond to the actual fields.
		--		2. count the rows and compare to the original file
		-- SELECT * FROM bronze.crm_cust_info
		-- SELECT COUNT(*) FROM bronze.crm_cust_info


		SET @start_time = GETDATE();
		PRINT '>> Truncating TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Pavlo\Portfolio-Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- Actual data starts from the second row, the first is the column names
			FIELDTERMINATOR = ',', -- Delimeter between the fields
			TABLOCK -- Locks the whole table during the insertion
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		-- SELECT * FROM bronze.crm_prd_info
		-- SELECT COUNT(*) FROM bronze.crm_prd_info

		SET @start_time = GETDATE();
		PRINT '>> Truncating TABLE: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Pavlo\Portfolio-Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- Actual data starts from the second row, the first is the column names
			FIELDTERMINATOR = ',', -- Delimeter between the fields
			TABLOCK -- Locks the whole table during the insertion
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		-- SELECT * FROM bronze.crm_sales_details
		-- SELECT COUNT(*) FROM bronze.crm_sales_details

		PRINT '-------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating TABLE: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Pavlo\Portfolio-Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, -- Actual data starts from the second row, the first is the column names
			FIELDTERMINATOR = ',', -- Delimeter between the fields
			TABLOCK -- Locks the whole table during the insertion
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		-- SELECT * FROM bronze.erp_cust_az12
		-- SELECT COUNT(*) FROM bronze.erp_cust_az12

		SET @start_time = GETDATE();
		PRINT '>> Truncating TABLE: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Pavlo\Portfolio-Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, -- Actual data starts from the second row, the first is the column names
			FIELDTERMINATOR = ',', -- Delimeter between the fields
			TABLOCK -- Locks the whole table during the insertion
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		-- SELECT * FROM bronze.erp_loc_a101
		-- SELECT COUNT(*) FROM bronze.erp_loc_a101

		SET @start_time = GETDATE();
		PRINT '>> Truncating TABLE: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Pavlo\Portfolio-Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, -- Actual data starts from the second row, the first is the column names
			FIELDTERMINATOR = ',', -- Delimeter between the fields
			TABLOCK -- Locks the whole table during the insertion
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		-- SELECT * FROM bronze.erp_px_cat_g1v2
		-- SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
