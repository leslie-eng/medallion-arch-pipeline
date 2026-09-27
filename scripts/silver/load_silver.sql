/*
 * ==========================================================
 * DDL Script: Create Silver Table
 * ==========================================================
 * Script Purpose:
 * 	This script creates tables in the 'bronze' schema, dropping 
 * 	existing tables if they already exists.
 * 		Run this script to re-define the DDL structure of 'bronze' Tables
 * 
 */
-- create the tables the silver layer
create or replace silver.load_silver()
language plpgsql
as $$
begin 
	drop table silver.olist_customers_dataset;
	create table if not exists silver.olist_customers_dataset(
		customer_id varchar(100),
		customer_unique_id varchar(100),
		customer_zip_code_prefix varchar(50),
		customer_city varchar(50),
		customer_state varchar(10));
	
	insert into silver.olist_customers_dataset 
	select 
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	initcap(customer_city),
	customer_state 
	from bronze.olist_customers_dataset;
	
	drop table silver.olist_geolocation_dataset;
	create table if not exists silver.olist_geolocation_dataset(
		geolocation_zip_code_prefix varchar(10),
		geolocation_lat varchar(100),
		geolocation_lng varchar(100),
		geolocation_city varchar(50),
		geolocation_state varchar(20));
	
	insert into silver.olist_geolocation_dataset
	select 
	geolocation_zip_code_prefix, 
	geolocation_lat, 
	geolocation_lng, 
	initcap(geolocation_city), 
	geolocation_state
	from bronze.olist_geolocation_dataset;
	
	drop table silver.olist_order_payments_dataset;
	create table silver.olist_order_payments_dataset(
		order_id varchar(100),
		payment_sequential integer,
		payment_type varchar(50),
		payment_installments integer,
		payment_value numeric(10, 2));
	
	insert into silver.olist_order_payments_dataset
	select 
	order_id,
	payment_sequential,
	case 
		when payment_type like '%_%' then regexp_replace(payment_type, '_',' ')
		else payment_type
	end,
	payment_installments,
	payment_value
	from bronze.olist_order_payments_dataset;
	
	drop table silver.olist_order_reviews_dataset;
	create table silver.olist_order_reviews_dataset(
		review_id varchar(100),
		order_id varchar(100),
		review_score integer,
		review_comment_title varchar,
		review_comment_message varchar,
		review_creation_date date,
		review_answer_timestamp date);
	
	insert into silver.olist_order_reviews_dataset
	select
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date::date,
	review_answer_timestamp::date
	from bronze.olist_order_reviews_dataset;
	
	-- this hasn't been executed yet
	drop table silver.olist_orders_dataset;
	create table silver.olist_orders_dataset(
	order_id varchar(100),
	customer_id varchar(100),
	order_status varchar(100),
	order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp);
	
	insert into silver.olist_orders_dataset
	select
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp::timestamp,
	order_approved_at::timestamp,
	order_delivered_carrier_date::timestamp,
	order_delivered_customer_date::timestamp,
	order_estimated_delivery_date::timestamp
	from bronze.olist_orders_dataset;
	
	-- olist_products_dataset
	drop table silver.olist_products_dataset; 
	create table silver.olist_products_dataset(
	product_id varchar(100),
	product_category_name varchar(100),
	product_name_lenght integer,
	product_description_lenght integer,
	product_photos_qty integer,
	product_weight_g integer,
	product_length_cm integer,
	product_height_cm integer,
	product_width_cm integer);
	
	insert into silver.olist_products_dataset
	select 
	* from bronze.olist_products_dataset;
	
	drop table silver.olist_sellers_dataset;
	create table silver.olist_sellers_dataset(
	seller_id varchar,
	seller_zip_code_prefix varchar(10),
	seller_city varchar(50),
	seller_state varchar(10));
	
	insert into silver.olist_sellers_dataset
	select
	seller_id,
	seller_zip_code_prefix,
	initcap(seller_city),
	seller_state 
	from bronze.olist_sellers_dataset;
	
	drop table if exists silver.product_category_name_translation;
	create table silver.product_category_name_translation(
	product_category_name varchar(100),
	product_category_name_english varchar(100));
	
	insert into silver.product_category_name_translation
	select
	product_category_name,
	case 
		when product_category_name_english like '%_%' then regexp_replace(product_category_name_english, '_',' ')
		else product_category_name_english
	end
	from bronze.product_category_name_translation;
		
	
	drop table silver.olist_order_items_dataset;
	create table silver.olist_order_items_dataset(
		order_id varchar(100),
		order_item_id integer,
		product_id varchar(100),
		seller_id varchar(100),
		shipping_limit_date varchar(50),
		price numeric(10, 2),
		freight_value numeric(10,2));
	
	insert into silver.olist_order_items_dataset
	select * from bronze.olist_order_items_dataset;
end;
$$;




