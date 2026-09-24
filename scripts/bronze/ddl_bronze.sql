/*
 * ==========================================================
 * DDL Script: Create Bronze Table
 * ==========================================================
 * Script Purpose:
 * 	This script creates tables in the 'bronze' schema, dropping 
 * 	existing tables if they already exists.
 * 		Run this script to re-define the DDL structure of 'bronze' Tables
 * 
 */

create or replace procedure bronze_layer_creation()
language plpgsql
as $$
begin
	drop table if exists bronze.olist_order_items_dataset;
	create table bronze.olist_order_items_dataset(
	order_id varchar(100),
	order_item_id integer,
	product_id varchar(100),
	seller_id varchar(100),
	shipping_limit_date varchar(50),
	price numeric(10, 2),
	freight_value numeric(10,2));
	
	drop table if exists bronze.olist_order_payments_dataset;
	create table bronze.olist_order_payments_dataset(
	order_id varchar(100),
	payment_sequential integer,
	payment_type varchar(50),
	payment_installments integer,
	payment_value numeric(10, 2));
	
	drop table if exists bronze.olist_order_reviews_dataset;
	create table bronze.olist_order_reviews_dataset(
	review_id varchar(100),
	order_id varchar(100),
	review_score integer,
	review_comment_title varchar,
	review_comment_message varchar,
	review_creation_date text,
	review_answer_timestamp text);
	
	drop table if exists bronze.olist_orders_dataset;
	create table bronze.olist_orders_dataset(
	order_id varchar(100),
	customer_id varchar(100),
	order_status varchar(100),
	order_purchase_timestamp text,
	order_approved_at text,
	order_delivered_carrier_date text,
	order_delivered_customer_date text,
	order_estimated_delivery_date text);
	
	drop table if exists bronze.olist_products_dataset;
	create table bronze.olist_products_dataset(
	product_id varchar(100),
	product_category_name varchar(100),
	product_name_lenght integer,
	product_description_lenght integer,
	product_photos_qty integer,
	product_weight_g integer,
	product_length_cm integer,
	product_height_cm integer,
	product_width_cm integer);
	
	drop table if exists bronze.olist_sellers_dataset;
	create table bronze.olist_sellers_dataset(
	seller_id varchar,
	seller_zip_code_prefix varchar(10),
	seller_city varchar(50),
	seller_state varchar(10));
	
	drop table if exists bronze.product_category_name_translation;
	create table bronze.product_category_name_translation(
	product_category_name varchar(100),
	product_category_name_english varchar(100));
	
	drop table if exists bronze.olist_customers_dataset;
	create table bronze.olist_customers_dataset(
	customer_id varchar(100),
	customer_unique_id varchar(100),
	customer_zip_code_prefix varchar(10),
	customer_city varchar(50),
	customer_state varchar(10));
	
	drop table if exists bronze.olist_geolocation_dataset;
	create table bronze.olist_geolocation_dataset(
	geolocation_zip_code_prefix varchar(100),
	geolocation_lat varchar(20),
	geolocation_lng varchar(20),
	geolocation_city varchar(50),
	geolocation_state varchar(10));
	
end;
$$;
