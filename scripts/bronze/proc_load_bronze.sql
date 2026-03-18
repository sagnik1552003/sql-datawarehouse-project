/*making a stored procedure for loading tables*/

EXEC bronze.load_bronze;
go

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
declare @start_time datetime, @end_time datetime
	begin try
	print 'LOADING BRONZE LAYER';
	print '========================='

	print 'LOADING CRM TABLES';

	set @start_time = getdate();
	truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from 'D:\All data\warehouse project\warehouse proj\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
	set @end_time= getdate();
	print 'time for loading crm_cust_info ' + cast (datediff(millisecond,@start_time,@end_time) as nvarchar) + ' milliseconds';
	print'-------------------------------------';


	set @start_time = getdate();
	truncate table bronze.crm_prd_info;
	bulk insert bronze.crm_prd_info
	from 'D:\All data\warehouse project\warehouse proj\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
	set @end_time= getdate();
	print 'time for loading crm_prd_info ' + cast (datediff(millisecond,@start_time,@end_time) as nvarchar) + ' milliseconds';
	print'-------------------------------------';


	set @start_time = getdate();
	truncate table bronze.crm_sales_details;
	bulk insert bronze.crm_sales_details
	from 'D:\All data\warehouse project\warehouse proj\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
    set @end_time= getdate();
	print 'time for loading crm_sales_details ' + cast (datediff(millisecond,@start_time,@end_time) as nvarchar) + ' milliseconds';
	print'-------------------------------------';


	print 'LOADING ERP TABLES';

	set @start_time= getdate();
	truncate table bronze.erp_cust_az12;
	bulk insert bronze.erp_cust_az12
	from 'D:\All data\warehouse project\warehouse proj\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
	set @end_time= getdate();
	print 'time for loading erp_cust_az12 ' + cast(datediff(millisecond, @start_time, @end_time) as nvarchar) + ' milliseconds';
	print'-------------------------------------';

	set @start_time= getdate();
	truncate table bronze.erp_loc_a101;
	bulk insert bronze.erp_loc_a101
	from 'D:\All data\warehouse project\warehouse proj\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
	set @end_time= getdate();
	print 'time for loading erp_loc_a101 ' + cast(datediff(millisecond, @start_time, @end_time) as nvarchar) + ' milliseconds';
	print'-------------------------------------';

	set @start_time= getdate();
	truncate table bronze.erp_px_cat_g1v2;
	bulk insert bronze.erp_px_cat_g1v2
	from 'D:\All data\warehouse project\warehouse proj\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
	set @end_time= getdate();
	print 'time for loading erp_px_cat_g1v2 ' + cast(datediff(millisecond, @start_time, @end_time) as nvarchar) + ' milliseconds';
	print'-------------------------------------';


	end try
	begin catch
		print 'error message' + error_message();
		print 'error message' + cast(error_state() as nvarchar);
		print 'error message' + cast(error_number() as nvarchar);
	end catch
END
