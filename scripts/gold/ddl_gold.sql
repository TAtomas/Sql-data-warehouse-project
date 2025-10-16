/*
Create Fact view
*/
Create VIEW gold.fact_sales AS
 select 
		sd.sls_ord_num as order_number,
		pr.pruduct_key, ----sk----
		cu.customer_key,----sk----
		sd.sls_order_dt as order_date,
		sd.sls_ship_dt as shiping_date,
		sd.sls_due_dt as due_date,
		sd.sls_sales as sales_amount,
		sd.sls_quantity as quantity,
		sd.sls_price as price
 from 
	silver.crm_sales_details sd 
 left join 
	gold.dim_products pr
 on sd.sls_prd_key=pr.product_number
 left join 
	gold.dim_customers cu
 on sd.sls_cust_id=cu.customer_id

/*
Create dim_prodect view
*/

CREATE OR ALTER   VIEW gold.dim_products AS
Select 
	ROW_NUMBER()over(order by pn.prd_start_dt,pn.prd_key) AS pruduct_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as sub_category,
	pc.maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
	
from 
	silver.crm_prd_info pn
left join 
	silver.erp_px_cat_g1v2 pc
on pc.id=pn.cat_id
where pn.prd_end_dt is null ---that mean that we need only currant data why i do not know !!!--


/*
Create dim_customer view
*/


CREATE VIEW gold.dim_customers	AS
Select 
	ROW_NUMBER()over(order by ci.cst_id) AS customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'Unknowns' then ci.cst_gndr
	else coalesce(ca.gen,'Unknowns')
	end as gender,
	ca.bdate as brithdate,
	ci.cst_create_date as create_date

from 
	silver.crm_cust_info ci
left join 
	silver.erp_cust_az12 ca
on ca.cid=ci.cst_key
left join 
	silver.erp_loc_a101 la
on la.cid=ci.cst_key





