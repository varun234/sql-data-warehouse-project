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
--Execute bronze.load_bronze
Create or Alter procedure bronze.load_bronze as
Begin
	Declare @start_time DATETIME, @end_time DATETIME ,@start_Btime DATETIME,@end_Btime datetime;
	Begin try -- for error handling
		set @start_Btime = Getdate();
		PRINT'Loading the Bronze Layer';
		PRINT '===========================================';

		PRINT '------------------------------------------';
		PRINT'Loading CRM TAbles';
		PRINT '------------------------------------------';

		set @start_time = GETDATE();
		PRINT'TRUNCATING TABLE bronze_crm_cust_info ';
		TRUNCATE TABLE bronze_crm_cust_info;
		PRINT'Inserting data into the Table bronze_crm_cust_info';
		BULK INSERT bronze_crm_cust_info
		FROM 'C:\Users\Varun\Desktop\New folder\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		Print 'Duration:' + CAST(DATEDIFF(Second,@start_time,@end_time) as NVARCHAR) + 'Seconds' ;

		set @start_time = Getdate()
		PRINT'TRUNCATING TABLE bronze_crm_prd_info ';
		TRUNCATE TABLE bronze_crm_prd_info;
		PRINT'Inserting data into the Table bronze_crm_prd_info';
		BULK INSERT bronze_crm_prd_info
		FROM 'C:\Users\Varun\Desktop\New folder\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = Getdate()
		print 'Duration: ' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;
		

		set @start_time = Getdate()
		PRINT'TRUNCATING TABLE bronze_crm_sales_details ';
		TRUNCATE TABLE bronze_crm_sales_details;
		PRINT'Inserting data into the Table bronze_crm_sales_details';
		BULK INSERT bronze_crm_sales_details
		FROM 'C:\Users\Varun\Desktop\New folder\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate();
		PRINT 'Duration' + Cast(Datediff(second,@start_time,@end_time) as NVARCHAR) + 'Seconds'

		PRINT '------------------------------------------';
		PRINT'Loading ERP TAbles';
		PRINT '------------------------------------------';

		PRINT'TRUNCATING TABLE bronze_erp_cust_az12 ';
		TRUNCATE TABLE bronze_erp_cust_az12;
		PRINT'Inserting data into the Table bronze_erp_cust_az12';
		BULK INSERT bronze_erp_cust_az12
		FROM 'C:\Users\Varun\Desktop\New folder\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	

		PRINT'TRUNCATING TABLE bronze_erp_loc_a101';
		TRUNCATE TABLE bronze_erp_loc_a101
		PRINT'Inserting data into the Table bronze_erp_loc_a101';
		BULK INSERT bronze_erp_loc_a101
		FROM 'C:\Users\Varun\Desktop\New folder\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	

		PRINT'TRUNCATING TABLE bronze_erp_px_cat_g1v2';
		TRUNCATE TABLE bronze_erp_px_cat_g1v2;
		PRINT'Inserting data into the Table bronze_erp_px_cat_g1v2';
		BULK INSERT bronze_erp_px_cat_g1v2
		FROM 'C:\Users\Varun\Desktop\New folder\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	set @end_Btime = Getdate();
	print 'Duration for loading whole bronze table' + Cast(datediff(second,@start_Btime,@end_Btime) as NVARCHAR) + ' ' +'Seconds';
	end try 
	begin catch
			print'====================================================='
			print'Error occured during loading the bronze layer'
			print('Error Message' + Error_Message());
			print('Error Message' + cast(Error_Number() AS NVARCHAR));
			print('Error Message' + cast(Error_State() AS NVARCHAR));
	end catch
end
