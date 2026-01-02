-------------------------------------
drop table bronze.crm_sales_details_staging;
drop table bronze.crm_prd_info_staging;
drop table bronze.crm_cust_info_staging;
drop table bronze.erp_loc_a101_staging;
drop table bronze.erp_cust_az12_staging;
drop table bronze.erp_px_cat_g1v2_staging;

drop table bronze.crm_sales_details;
drop table bronze.crm_prd_info;
drop table bronze.crm_cust_info;
drop table bronze.erp_loc_a101;
drop table bronze.erp_cust_az12;
drop table bronze.erp_px_cat_g1v2;

drop table silver.crm_sales_details;
drop table silver.crm_prd_info;
drop table silver.crm_cust_info;
drop table silver.erp_loc_a101;
drop table silver.erp_cust_az12;
drop table silver.erp_px_cat_g1v2;

drop view gold.dim_sales;
drop view gold.dim_products;
drop view gold.dim_customers;

drop table gold.sales;
drop table gold.products;
drop table gold.customers;
----------------------------------------------------------------------


-----Validate Bronze, Silver and Gold Tables/Views-----
----------------------------------------------------------
------------------Bronze Layer---------------------------
select * from bronze.crm_cust_info limit 5;
select * from bronze.crm_prd_info limit 5;
select * from bronze.crm_sales_details limit 5;
select * from bronze.erp_loc_a101 limit 5;
select * from bronze.erp_cust_az12 limit 5;
select * from bronze.erp_px_cat_g1v2 limit 5;

----------------Bronze Staging------------------------------------
select * from bronze.crm_cust_info_staging;
select * from bronze.crm_cust_info_staging;
select * from bronze.crm_cust_info_staging

-----------------Gold Views-------------------------------------
select * from gold.dim_customers;
select * from gold.dim_products;
select * from gold.dim_sales;

-----------------Gold Tables-------------------------------------
select * from gold.customers;
select * from gold.products;
select * from gold.sales;

-----------------Silver Tables-------------------------------------
select * from silver.crm_cust_info;
select * from silver.crm_prd_info;
select * from silver.crm_sales_details;
select * from silver.erp_loc_a101;
select * from silver.erp_cust_az12;
select * from silver.erp_px_cat_g1v2;

------------------------------------------------
select * 
from silver.crm_sales_details
where sls_prd_key = 'BB-7421';

--------------------------------------------
select pn.*, null as space, pc.*
from bronze.crm_prd_info as pn
---silver.erp_px_cat_g1v2 as pc
left join bronze.erp_px_cat_g1v2 as pc
on REPLACE(SUBSTRING(pn.prd_key FROM 1 FOR 5), '-', '_') = pc.id
---where pn.prd_key = 'BB-7421';

select pn.*, null as space, pc.*
from silver.crm_prd_info as pn
---silver.erp_px_cat_g1v2 as pc
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id;