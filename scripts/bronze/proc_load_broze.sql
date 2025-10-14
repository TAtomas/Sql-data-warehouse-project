/*
Create proc to Load the data in the bronze table and handel error
calcolate the time of loading the data <>
Use the proc :
exec bronze.load_bronze <>
*/

Create or Alter procedure bronze.load_bronze 
as 
begin  
	begin try
	Declare @start_time Datetime,@end_time Datetime;
	Declare @all_start_time Datetime,@all_end_time Datetime;

		set @all_start_time=GETDATE();
	print '~~~~~~~~~~~~~~~~~~~~~~~~~~~';
	Print 'Loading Bronze Layre Data';
	print '~~~~~~~~~~~~~~~~~~~~~~~~~~~';


		set @start_time=GETDATE();
		truncate table bronze.crm_cust_info;
		Bulk insert bronze.crm_cust_info
		from 'D:\DWH_Project\DataOF DWH Braa Project\source_crm\cust_info.csv'
		with 
		(
		firstrow=2,
		fieldterminator=',',
		tablock
		);

		set @end_time =Getdate()
		print'Load Time Take >'+ cast(DateDiff(second,@start_time,@end_time) as Nvarchar)+'Second';



		set @start_time=GETDATE();
		truncate table bronze.erp_cust_az12;
		Bulk insert bronze.erp_cust_az12
		from 'D:\DWH_Project\DataOF DWH Braa Project\source_erp\CUST_AZ12.csv'
		with 
		(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =Getdate()
		print'Load Time Take >'+ cast(DateDiff(second,@start_time,@end_time) as Nvarchar)+'Second'

		



		set @start_time=GETDATE();
		truncate table bronze.crm_prd_info;
		Bulk insert bronze.crm_prd_info
		from 'D:\DWH_Project\DataOF DWH Braa Project\source_crm\prd_info.csv'
		with 
		(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =Getdate()
		print'Load Time Take >'+ cast(DateDiff(second,@start_time,@end_time) as Nvarchar)+'Second'

	





	    set @start_time=GETDATE();
		truncate table bronze.crm_sales_details;
		Bulk insert bronze.crm_sales_details
		from 'D:\DWH_Project\DataOF DWH Braa Project\source_crm\sales_details.csv'
		with 
		(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =Getdate()
		print'Load Time Take >'+ cast(DateDiff(second,@start_time,@end_time) as Nvarchar)+'Second'
	



	    set @start_time=GETDATE();
		truncate table bronze.erp_loc_a101;
		Bulk insert bronze.erp_loc_a101
		from 'D:\DWH_Project\DataOF DWH Braa Project\source_erp\LOC_A101.csv'
		with 
		(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =Getdate()
		print'Load Time Take >'+ cast(DateDiff(second,@start_time,@end_time) as Nvarchar)+'Second'
	

	    set @start_time=GETDATE();
		truncate table bronze.erp_px_cat_g1v2;
		Bulk insert bronze.erp_px_cat_g1v2
		from 'D:\DWH_Project\DataOF DWH Braa Project\source_erp\PX_CAT_G1V2.csv'
		with 
		(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =Getdate()
		print'Load Time Take >'+ cast(DateDiff(second,@start_time,@end_time) as Nvarchar)+'Second'

		set @all_end_time=GETDATE();
		print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
		print 'LOADING BRONZE LAYER IS COMBLETED'
		print'Patch Load Time Take >'+ cast(DateDiff(second,@start_time,@end_time) as Nvarchar)+'Second'
		print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
	end try
	begin catch
		print'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';
		print'An Error Occured During Loading the Data';
		print 'Error message'+ERROR_MESSAGE();
		print'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';
	end catch
		
end

