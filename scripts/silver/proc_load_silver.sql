/*
=============================================================
Procedure Name: silver.load_silver
Purpose: 
    - Load and transform data from Bronze to Silver layer.
    - Perform data cleaning, standardization, and quality checks.
    - Handle errors gracefully and log total load time for each dataset.

Features:
    • Data Cleaning:
        - Remove duplicates using ranking logic.
        - Trim spaces and normalize text fields.
        - Replace invalid or inconsistent values with standard ones.
        - Fix invalid dates and handle missing values.
        - Enforce data consistency across Silver tables.
    • Error Handling:
        - Catch and display error messages.
    • Performance Tracking:
        - Calculate and display the time taken to load each table 
          and the total duration for the entire load process.

Usage:
    EXEC bronze.load_bronze;  -- Load Bronze layer first
    EXEC silver.load_silver;  -- Then load Silver layer
=============================================================
*/

CREATE OR ALTER PROCEDURE  silver.load_silver as
begin
	begin try
	Declare @start_time Datetime,@end_time Datetime;
	Declare @all_start_time Datetime,@all_end_time Datetime;
	  set @all_start_time =getdate()
		print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
		print 'INSERTING DATA INTO SILVER LAYER'
		print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
		print'<><><><><><><><><><><><><><><><><><><><><>'
		print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
		print 'LOADING CRM DATA INTO SILVER LAYER'
		print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
		 set @start_time= GETDATE();
		print 'INSERTING DATA INTO  silver.crm_cust_info :'
		truncate table  silver.crm_cust_info
		insert into silver.crm_cust_info
		(
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
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case
				when upper(Trim(cst_marital_status))= 'S' then 'Single'
				when upper(Trim(cst_marital_status))='M' then 'Married'
				else 'Unknowns'
			end cst_marital_status,
			case
				when upper(Trim(cst_gndr))= 'M' then 'Male'
				when upper(Trim(cst_gndr))='F' then 'Female'
				else 'Unknowns'
			end cst_gndr,
			cst_create_date
		from(
		Select 
			*,
			ROW_NUMBER()over(Partition by cst_id order by cst_create_date desc) as ranking_list
		from bronze.crm_cust_info
		where cst_id is not null
		)  flag_rank
		where flag_rank.ranking_list = 1
		   set @end_time =Getdate()
		   print 'Load Time Take : ' +cast(datediff(second,@start_time,@end_time) as nvarchar)+'Second';
		PRINT''
		
		----------------------------------------------

		 set @start_time= GETDATE();
		print 'INSERTING DATA INTO  silver.crm_prd_info : '
		truncate table silver.crm_prd_info
		INSERT INTO silver.crm_prd_info(prd_id,
			cat_id,
			prd_key,
			prd_cost,
			prd_line,
			prd_nm,
			prd_start_dt,
			prd_end_dt
		)
		select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		isnull(prd_cost,0)as prd_cost,
		prd_line,
		case when upper(trim(prd_line)) ='M' then 'Mountain'
		when upper(trim(prd_line)) ='R' then 'Road'
		when upper(trim(prd_line)) ='S' then 'Other Sales'
		when upper(trim(prd_line)) ='T' then 'Touring'
		else 'Unknowns'
		end prd_nm,
		cast(prd_start_dt AS date) as prd_start_dt,
		cast(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 AS date) as prd_end_dt
		from bronze.crm_prd_info
		  set @end_time =Getdate()
		  print 'Load Time Take : ' +cast(datediff(second,@start_time,@end_time) as nvarchar)+'Second';

		PRINT''
		 
		------------------------------------------------

		  set @start_time= GETDATE();
		print 'INSERTING DATA INTO  silver.crm_sales_details : '
		truncate table silver.crm_sales_details
		Insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)

		Select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case 
				when sls_order_dt <=0 OR LEN(sls_order_dt)!=8 then NULL
				else CAST(cast(sls_order_dt AS nvarchar) AS date)
			end sls_order_dt ,
			case 
				when sls_ship_dt <=0 OR LEN(sls_ship_dt)!=8 then NULL
				else CAST(cast(sls_ship_dt AS nvarchar) AS date)
			end sls_ship_dt ,
			case 
				when sls_due_dt <=0 OR LEN(sls_due_dt)!=8 then NULL
				else CAST(cast(sls_due_dt AS nvarchar) AS date)
			end sls_due_dt ,
			case when sls_sales IS null Or sls_sales<=0 or sls_sales!= sls_quantity* ABS(sls_price)
			then  sls_quantity* ABS(sls_price)
			else sls_sales
			end  sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price <=0 
			then sls_sales/nullif(sls_quantity,0)
			else sls_price
			end sls_price
		from bronze.crm_sales_details
		   set @end_time =Getdate()
		   print 'Load Time Take : ' +cast(datediff(second,@start_time,@end_time) as nvarchar)+'Second';
		PRINT''
		------------------------------------------
		print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
		print 'LOADING ERP DATA INTO SILVER LAYER'
		print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
		 set @start_time= GETDATE();
		print 'INSERTING DATA INTO  silver.erp_cust_az12 : '
		Truncate table silver.erp_cust_az12
		insert into silver .erp_cust_az12
		(
			cid,
			bdate,
			gen
		)
		select 
		case
			when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid))
			else cid 
		end cid,
		case
			when bdate > getdate() then NULL
			else bdate
		end bdate,
		case
			when UPPER (Trim(gen))='M'OR upper(Trim(gen))='MALE' then 'Male'
			when UPPER (Trim(gen))='F'OR upper(Trim(gen))='FEMALE' then 'Female'
			else 'Unknowns'
		end gen
		from bronze.erp_cust_az12
		   set @end_time =Getdate()
		   print 'Load Time Take : ' +cast(datediff(second,@start_time,@end_time) as nvarchar)+'Second';

		PRINT''
		-----------------------------------------------
		 set @start_time= GETDATE();
		print 'INSERTING DATA INTO  silver.erp_loc_a101 : '
		truncate table silver.erp_loc_a101
		insert into silver.erp_loc_a101(cid,cntry)
		select 
		trim(replace (cid,'-','') )as cid,
		case 
			when UPPER(trim(cntry)) IN('USA','US','UNITED STATES') then 'United States'
			when UPPER(trim(cntry)) IN('DE','GERMANY') then 'Germany'
			when trim(cntry)= '' or trim(cntry) IS null then 'Unknowns'
			else TRIM (cntry)
		end as cntry
		from bronze.erp_loc_a101
		   set @end_time =Getdate()
		   print 'Load Time Take : ' +cast(datediff(second,@start_time,@end_time) as nvarchar)+'Second';
		PRINT''
		----------------------------------------------------


		 set @start_time= GETDATE();
		print 'INSERTING DATA INTO  silver.erp_px_cat_g1v2 : '
		truncate table silver.erp_px_cat_g1v2
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select 
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2
		 set @end_time =Getdate()
		 print 'Load Time Take : ' +cast(datediff(second,@start_time,@end_time) as nvarchar)+'Second';

		PRINT''
		  print '~~~~~~~~~~~~~~~~~~~~~~~~~'
		  print 'ALL DATA LOADED SUCCESSFULLY'
	   	  print 'Loading all data Take : ' +cast(datediff(second,@all_start_time,GETDATE()) as nvarchar)+' Second';
		  print '~~~~~~~~~~~~~~~~~~~~~~~~~'

	end try
	begin catch
		print'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';
		print'An Error Occured During Loading the Data';
		print 'Error message'+ERROR_MESSAGE();
		print'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';
	end catch
End


