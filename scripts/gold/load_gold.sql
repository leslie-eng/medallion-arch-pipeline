/*
 * ==========================================================
 * DDL Script: Create Gold Views
 * ==========================================================
 * Script Purpose:
 * 	This script creates a snowflake schema in the 'gold' schema as
 * 	views over the 'silver' tables. Views are dropped and recreated,
 * 	so the script is safe to re-run.
 *
 * 	fact_orders ──▶ dim_customer
 * 	     │
 * 	     └──────▶ dim_products ──▶ dim_sellers
 *
 */
create schema if not exists gold;

-- drop in dependency order: fact → products → sellers
drop view if exists gold.fact_orders;
drop view if exists gold.fact_order_items;
drop view if exists gold.order_fact_table;
drop view if exists gold.dim_customer;
drop view if exists gold.dim_products;
drop view if exists gold.dim_sellers;

--sellers table with all the data
-- geolocation has many rows per zip prefix, so it is reduced to one row per zip
create view gold.dim_sellers as
	with geo as (
		select
		geolocation_zip_code_prefix,
		avg(geolocation_lat::double precision) as lat,
		avg(geolocation_lng::double precision) as lng
		from silver.olist_geolocation_dataset
		group by geolocation_zip_code_prefix
	)
	select
	row_number() over(order by osd.seller_id) as seller_key,
	osd.seller_id,
	osd.seller_zip_code_prefix as zip_code,
	osd.seller_city as city,
	osd.seller_state as state,
	geo.lat,
	geo.lng
	from silver.olist_sellers_dataset osd
	left join geo
	on osd.seller_zip_code_prefix = geo.geolocation_zip_code_prefix;

-- products table with all the data, linked to its seller
-- the same product can be sold by several sellers, so there is one row per product/seller pair
create view gold.dim_products as
	with product_sellers as (
		select distinct
		product_id,
		seller_id
		from silver.olist_order_items_dataset
	)
	select
	row_number() over(order by ps.product_id, ps.seller_id) as product_key,
	ps.product_id,
	ds.seller_key,
	cnt.product_category_name_english as category_name,
	opd.product_name_lenght as product_name_length,
	opd.product_description_lenght as description_length,
	opd.product_photos_qty as photo_count,
	opd.product_weight_g as product_weight,
	opd.product_length_cm as product_length,
	opd.product_height_cm as product_height,
	opd.product_width_cm as product_width
	from product_sellers ps
	join silver.olist_products_dataset opd
	on ps.product_id = opd.product_id
	left join silver.product_category_name_translation cnt
	on opd.product_category_name = cnt.product_category_name
	left join gold.dim_sellers ds
	on ps.seller_id = ds.seller_id;

-- customer table with all the data
create view gold.dim_customer as
	with geo as (
		select
		geolocation_zip_code_prefix,
		avg(geolocation_lat::double precision) as lat,
		avg(geolocation_lng::double precision) as lng
		from silver.olist_geolocation_dataset
		group by geolocation_zip_code_prefix
	)
	select
	row_number() over(order by ocd.customer_id) as customer_key,
	ocd.customer_id,
	ocd.customer_unique_id as customer_uid,
	ocd.customer_zip_code_prefix as zip_code,
	ocd.customer_city as city,
	ocd.customer_state as state,
	geo.lat,
	geo.lng
	from silver.olist_customers_dataset ocd
	left join geo
	on ocd.customer_zip_code_prefix = geo.geolocation_zip_code_prefix;

-- order fact table: one row per order item, linked to the customer and the product
-- payments and reviews are per order, so they are aggregated per order before joining
create view gold.fact_orders as
	with payments as (
		select
		order_id,
		string_agg(distinct payment_type, ', ') as payment_types,
		max(payment_installments) as payment_installments,
		sum(payment_value) as payment_value
		from silver.olist_order_payments_dataset
		group by order_id
	),
	reviews as (
		-- keep the latest review per order
		select distinct on (order_id)
		order_id,
		review_id,
		review_score,
		review_comment_title,
		review_comment_message,
		review_creation_date,
		review_answer_timestamp
		from silver.olist_order_reviews_dataset
		order by order_id, review_answer_timestamp desc
	)
	select
	ooid.order_id,
	ooid.order_item_id,
	dc.customer_key,
	dp.product_key,
	ood.order_status,
	ood.order_purchase_timestamp as purchase_timestamp,
	ood.order_approved_at as approval_timestamp,
	ood.order_delivered_carrier_date as delivered_carrier_date,
	ood.order_delivered_customer_date as delivered_customer_date,
	ood.order_estimated_delivery_date as estimated_delivery_date,
	ooid.shipping_limit_date::timestamp as shipping_limit_date,
	ooid.price,
	ooid.freight_value,
	ooid.price + ooid.freight_value as total_value,
	p.payment_types,
	p.payment_installments,
	-- the order's payment split across its items by value, so it can be summed without double counting
	round(p.payment_value * (ooid.price + ooid.freight_value)
		/ nullif(sum(ooid.price + ooid.freight_value) over(partition by ooid.order_id), 0), 2) as payment_value,
	r.review_id,
	r.review_score as score,
	r.review_comment_title as comment_title,
	r.review_comment_message as comment_message,
	r.review_creation_date,
	r.review_answer_timestamp
	from silver.olist_order_items_dataset ooid
	join silver.olist_orders_dataset ood
	on ooid.order_id = ood.order_id
	left join gold.dim_customer dc
	on ood.customer_id = dc.customer_id
	left join gold.dim_sellers ds
	on ooid.seller_id = ds.seller_id
	left join gold.dim_products dp
	on ooid.product_id = dp.product_id
	and ds.seller_key = dp.seller_key
	left join payments p
	on ooid.order_id = p.order_id
	left join reviews r
	on ooid.order_id = r.order_id;
