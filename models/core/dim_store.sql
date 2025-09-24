{{
    config(
        materialized='table'
    )
}}

WITH sub AS (
    SELECT store_id,
    store_name,
    store_region,
    store_rating,
    ROW_NUMBER() OVER(PARTITION BY store_id ORDER BY store_id) AS rn
FROM {{ ref('stg_sales') }}
)

SELECT
  store_id,
  store_name,
  store_region,
  store_rating
FROM sub
WHERE rn=1
