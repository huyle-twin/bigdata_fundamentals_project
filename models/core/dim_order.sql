{{
    config(
        materialized='table'
    )
}}

WITH sub AS (
    SELECT 
    order_id,
    order_datetime,
    order_status,
    coupon_code,
    ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY order_id) AS rn
FROM {{ ref('stg_sales') }}
)

SELECT 
    order_id,
    order_datetime,
    order_status,
    coupon_code
FROM sub
WHERE rn=1