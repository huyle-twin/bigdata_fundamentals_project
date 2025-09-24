{{
    config(
        materialized='table'
    )
}}

WITH sub AS (
    SELECT product_sku, product_name, brand, category_name, list_price, color, weight_g,
    ROW_NUMBER() OVER(PARTITION BY product_sku ORDER BY product_sku) AS rn
    FROM {{ ref('stg_sales') }}
)

SELECT product_sku, product_name, brand, category_name, list_price, color, weight_g
FROM sub
WHERE rn=1