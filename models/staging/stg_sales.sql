{{
    config(
        materialized='table',
        schema = 'gold'
    )
}}

SELECT order_id, 
order_line_id, order_datetime, order_status, coupon_code, customer_id, 
customer_full_name, customer_email, customer_gender, customer_region, 
customer_signup_date, store_id, store_name, store_region, store_rating,
product_sku, product_name, brand, category_name, qty, list_price, 
unit_price, discount AS discount_percent, line_amount, color, 
COALESCE(CAST(weight_g AS DECIMAL)) AS weight_g
FROM {{ source('staging', 'cleaned_table') }}