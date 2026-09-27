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


-- customer table with all the data
create view dim_customer as
	select
	row_number() OVER(order by ocd.customer_id) as customer_key,
	ocd.customer_id,
	ocd.customer_unique_id as customer_uid,
	ocd.customer_zip_code_prefix as zip_code,
	ocd.customer_city as city,
	ocd.customer_state as state,
	ogd.geolocation_lat as lat,
	ogd.geolocation_lng as lng
	from silver.olist_customers_dataset ocd 
	join silver.olist_geolocation_dataset ogd 
	on ocd.customer_zip_code_prefix = ogd.geolocation_zip_code_prefix;

-- products table with all the data
create view dim_products as 
	select
	row_number() over(order by opd.product_id) as product_key
	opd.product_id,
	cnt.product_category_name_english as product_name_length,
	opd.product_name_lenght as product_name_length,
	opd.product_description_lenght as description_length,
	opd.product_photos_qty as quantity,
	opd.product_weight_g as product_weight,
	opd.product_length_cm as product_length,
	opd.product_width_cm as product_width
	from silver.olist_products_dataset opd 
	join silver.product_category_name_translation cnt 
	on opd.product_category_name = cnt.product_category_name; 

--sellers table with all the data
create view gold.dim_sellers as 
	select 
	osd.seller_id,
	osd.seller_zip_code_prefix as zip_code,
	osd.seller_city as city,
	osd.seller_state as state,
	ogd.geolocation_lat as lat,
	ogd.geolocation_lng as lng
	from silver.olist_sellers_dataset osd
	join silver.olist_geolocation_dataset ogd 
	on osd.seller_zip_code_prefix = ogd.geolocation_zip_code_prefix;

-- order fact table
create view gold.order_fact_table as
	select 
	ood.order_id,
	ood.customer_id,
	ood.order_status,
	ood.order_purchase_timestamp as purchase_timestamp,
	ood.order_approval_at as approval_timestamp,
	ood.order_delivered_carrier_date as delivered_carrier_date,
	ood.order_delivered_customer_date as delivered_customer_date,
	ood.order_estimated_delivery_date as estimated_delivery_date,
	oopd.payment_sequential as payment_sequence,
	oopd.payment_type,
	oopd.payment_installments,
	oopd.payment_value,
	oord.review_id,
	oord.review_score as score,
	oord.review_comment_title as comment_title,
	oord.review_comment_message as coment_message,
	oord.review_creation_date,
	oord.review_answer_timestamp
	from silver.olist_orders_dataset ood 
	join silver.olist_order_payments_dataset oopd 
	on ood.order_id = oopd.order_id
	join silver.olist_order_reviews_dataset oord 
	on ood.order_id = oord.order_id;






