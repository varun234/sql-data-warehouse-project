/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
create view gold.dim_customer as(
select 
row_number() over(order by cst_id ) as customer_key, -- surrogate key since it is dimension table
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status ,
--cst_gndr,
case when cst_gndr != 'N/A' then cst_gndr
	else coalesce(gen,'N/A')
end as New_gender,
ca.bdate as birth_date,
ci.cst_create_date as create_date 
--ca.gen,
from silver_crm_cust_info ci
left join silver_erp_cust_az12 ca
on ca.cid = ci.cst_key
left join silver_erp_loc_a101 la
on la.cid = ci.cst_key
)

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as (
SELECT 
row_number() over (order by cpi.prd_start_dt,cpi.prd_key) as product_key, -- since it is a dimension we need to create a surrogate key
cpi.prd_id as product_id,
cpi.prd_key as product_number,
cpi.prd_nm as product_name,
cpi.cat_id as category_id,
pcg.cat as category,
pcg.subcat as subcategory,
pcg.maintenance,
cpi.prd_cost as product_cost,
cpi.prd_line as product_line,
cpi.prd_start_dt as start_dt
--cpi.prd_end_dt, -- any how it is null so commented out, no need of end date
FROM silver_crm_prd_info cpi
left join silver_erp_px_cat_g1v2 pcg
on pcg.id = cpi.cat_id
where prd_end_dt is null -- to filter out historical data and have only current data
)

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
  
create view gold.fact_sales as (
select 
sls_ord_num as order_number,
pr.product_key,--sls_prd_key,replacing this with surrogate key of gold.dim_products since it is main fact table which will be connected by dimensions.
cu.customer_key,--sls_cust_id, replacing this with surrogate key of gold.dim_customer since it is main fact table which will be connected by dimensions.
sls_order_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as sales_date,
sls_sales as sales_amount,
sls_quantity as quantity,
sls_price as price
from silver_crm_sales_details sd
left join gold.dim_customer cu
on cu.customer_id   = sd.sls_cust_id
left join gold.dim_products pr
on pr.product_number   = sd.sls_prd_key
)
