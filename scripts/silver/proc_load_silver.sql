/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
Create or alter procedure silver.Load_silver AS
Begin
	
	Begin try
		declare @start_time datetime, @end_time datetime, @startBatch_time datetime,@endBatch_Time datetime ;
		set @startBatch_time = getdate();

		Print'Loading the silver layer';
		print'------------------------------------';

		set @start_time = getdate();
		print 'Truncating table silver_crm_cust_info ';
		Truncate table silver_crm_cust_info;
		print 'Loading table silver_crm_cust_info ';
		insert into silver_crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select 
				cst_id,
				cst_key,
				trim(cst_firstname),
				trim(cst_lastname),
				case when Upper(trim(cst_marital_status)) = 's' then 'Single'
					 when Upper(trim(cst_marital_status)) = 'M' then 'Married'
					 else 'N/a'
				end cst_marital_status,
				case when Upper(trim(cst_gndr)) = 'f' then 'Female'
					 when Upper(trim(cst_gndr)) = 'm' then 'Male'
					else 'N/A'
				end cst_gndr,
				cst_create_date
	FROM (
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze_crm_cust_info
		WHERE cst_id IS NOT NULL
	) t
	WHERE flag_last = 1;
		set @end_time = Getdate() ;
		print' time taken to load silver_crm_cust_info =' + cast(datediff(second,@start_time,@end_time) as Nvarchar) + '' +'Seconds';
		Print '---------------------------------';

		set @start_time = Getdate();
		print 'Truncating table silver_crm_prd_info ';
		truncate table silver_crm_prd_info;
		print 'Loading table silver_crm_prd_info ';
		INSERT into silver_crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm ,
		prd_cost,
		prd_line ,
		prd_start_dt, 
		prd_end_dt
		)
		select 
		prd_id,
		Replace(substring(prd_key,1,5),'-','_') as cat_id,-- extract category ID   derived columns = create columns based on calc or tranformations of existing ones.
		substring(prd_key,7,len(prd_key)) as prd_key, -- extract product key
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		case when UPPER(trim(prd_line)) = 'R' then 'Road'
			 when UPPER(trim(prd_line)) = 'M' then 'Mountain'
			 when UPPER(trim(prd_line)) = 'T' then 'Trail'
			 when UPPER(trim(prd_line)) = 'S' then 'Other sales'
		else 'N/A' -- data normalisation - map product line cosed to descriptive values
		end as prd_line,
		CAST(prd_start_dt AS DATE),-- SINCE THERE WAS NO TIME, CHANGED IT TO ONLY DATE
		--prd_end_dt
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY PRD_KEY ORDER BY prd_start_dt)-1 AS DATE )AS prd_end_dt -- calcaulated end date as one day before the next start date
		FROM bronze_crm_prd_info 
		-- line 25 to 27 - data enrichment
		-- line 24  data transformation
		set @end_time = Getdate();
		print' time taken to load silver_crm_prd_info =' + cast(datediff(second,@start_time,@end_time) as Nvarchar) + '' +'Seconds';
		Print '---------------------------------';

		set @start_time = Getdate();
		print 'Truncating table silver_crm_sales_details ';
		Truncate table silver_crm_sales_details ; 
		print 'Loading table silver_crm_sales_details ';
		Insert into silver_crm_sales_details(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price )
		select   
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or Len(sls_order_dt) != 8 then Null
			 else Cast(cast(sls_order_dt as VARCHAR) as Date)
		end AS sls_order_dt,
		case when sls_ship_dt = 0 or Len(sls_ship_dt) != 8 then Null
			 else Cast(cast(sls_ship_dt as VARCHAR) as Date)
		end AS sls_ship_dt,
		case when sls_due_dt = 0 or Len(sls_due_dt) != 8 then Null
			 else Cast(cast(sls_due_dt as VARCHAR) as Date)
		end AS sls_due_dt,  
		case when sls_sales <=0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price ) then sls_quantity * abs(sls_price )
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		Case when sls_price <= 0 or sls_price is null then  sls_sales / Nullif(sls_quantity,0)
			 else sls_price
		end as sls_price 
		from bronze_crm_sales_details
		set @end_time = getdate();
		print' time taken to load silver_crm_sales_details =' + cast(datediff(second,@start_time,@end_time) as Nvarchar) + '' +'Seconds';
		Print '---------------------------------';

		set @start_time = Getdate();
		print 'Truncating table silver_erp_cust_az12 ';
		truncate table silver_erp_cust_az12;
		print 'Loading table silver_erp_cust_az12 ';
		Insert into silver_erp_cust_az12(
		cid,
		bdate,
		gen)
		select 
		case when cid like 'NAS%' then substring(cid,4,Len(cid))
			 else cid
		end as cid,
		case when bdate > GetDATE() then NULL
		else bdate
		end as bdate,
		case when trim(upper(gen)) in ('M','Male') then 'Male'
			 when trim(upper(gen)) in ('F','Female') then 'Female'
			 else 'N/A'
		end as gen 
		from bronze_erp_cust_az12
		set @end_time = Getdate();
		print' time taken to load silver_erp_cust_az12 =' + cast(datediff(second,@start_time,@end_time) as Nvarchar) + '' +'Seconds';
		Print '---------------------------------';

		set @start_time = getdate();
		print 'Truncating table silver_erp_loc_a101 ';
		truncate table silver_erp_loc_a101;
		print 'Loading table silver_erp_loc_a101 ';
		insert into silver_erp_loc_a101(
		cid,
		cntry)
		Select
		replace(cid,'-','') as cid,
		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US','USA') then 'United States'
			 WHEN trim(cntry) is null or cntry = '' then 'N/A'
			 else trim(cntry)
		end as cntry
		from bronze_erp_loc_a101 
		set @end_time = getdate();
		print' time taken to load silver_erp_loc_a101 =' + cast(datediff(second,@start_time,@end_time) as Nvarchar) + '' +'Seconds';
		Print '---------------------------------';

		set @start_time = getdate();
		print 'Truncating table silver_erp_px_cat_g1v2 ';
		truncate table silver_erp_px_cat_g1v2;
		print 'Loading table silver_erp_px_cat_g1v2 ';
		INSERT INTO silver_erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		from bronze_erp_px_cat_g1v2;
		set @end_time = Getdate();
		print' time taken to load silver_erp_px_cat_g1v2 =' + cast(datediff(second,@start_time,@end_time) as Nvarchar) + '' +'Seconds';

		set @endBatch_Time = GetDate();
		print'Total time taken to load silver_layer =' + cast(datediff(second,@startBatch_time,@endBatch_Time) as Nvarchar) + '' +'Seconds';
	end try
	begin catch
			print'=====================================================';
			print'Error occured during loading the bronze layer';
			print('Error Message' + Error_Message());
			print('Error Message' + cast(Error_Number() AS NVARCHAR));
			print('Error Message' + cast(Error_State() AS NVARCHAR));
	end catch
end

