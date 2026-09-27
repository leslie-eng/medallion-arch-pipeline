
-- checking olist_customers_dataset
-- do a sanity check of the data
select * from bronze.olist_customers_dataset limit 10;

-- how many records do we have: 99441
select count(customer_id) as customer_id_count
from bronze.olist_customers_dataset;

-- checking the data has duplicates
-- customer_id
select customer_id, count(customer_id) as customer_id_count
from bronze.olist_customers_dataset
group by customer_id
having count(customer_id) > 1;

-- customer_unique_id
-- why does customer_unique_id have duplicate data
select customer_unique_id, count(customer_unique_id) as customer_unique_id_count
from bronze.olist_customers_dataset
group by customer_unique_id
having count(customer_unique_id) > 1;

select count(distinct(customer_zip_code_prefix)) count_of_distinct_zip_codes
from bronze.olist_customers_dataset;

-- normalize the customer_city
select distinct(customer_city) from bronze.olist_customers_dataset;
select initcap(customer_city) from bronze.olist_customers_dataset;

-- customer_state - 27
select distinct(customer_state) from bronze.olist_customers_dataset;

-- checking olist_geolocation_dataset
select * from bronze.olist_geolocation_dataset limit 10;

-- checking whether geolocation_city is capitalized
select initcap(geolocation_city) from bronze.olist_geolocation_dataset limit 10;

-- checking payment dataset
select * from silver.olist_order_payments_dataset limit 10;

-- checking the type of payment
select distinct(payment_type) from bronze.olist_order_payments_dataset;

select 
case 
	when payment_type like '%_%' then regexp_replace(payment_type, '_',' ')
	else payment_type
end
from silver.olist_order_payments_dataset limit 10;

-- checking order reviews dataset
select * from silver.olist_order_reviews_dataset limit 10;

-- converting review creation date into timestamp
select * from silver.olist_order_reviews_dataset limit 10;

select * from silver.olist_order_reviews_dataset
where review_answer_timestamp::date < review_creation_date::date; 

select * from silver.olist_products_dataset;

select * from silver.olist_sellers_dataset;

select * from silver.product_category_name_translation;

select * from silver.olist_orders_dataset;

select order_approved_at
from silver.olist_orders_dataset 
where order_purchase_timestamp::date > order_approved_at::date ;


