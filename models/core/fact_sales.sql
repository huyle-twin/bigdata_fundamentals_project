{{
    config(
        materialized='table'
    )
}}

SELECT order_line_id, order_id, order_datetime AS order_date,
customer_id, store_id, product_sku, qty, unit_price, discount_percent,
line_amount
FROM {{ ref('stg_sales') }}