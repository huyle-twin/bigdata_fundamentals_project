{{
    config(
        materialized='table'
    )
}}

WITH sub AS (
    SELECT customer_id, customer_full_name, customer_email, customer_gender, customer_region,
    customer_signup_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY customer_id) AS rn
    FROM {{ ref('stg_sales') }}
)

SELECT customer_id, customer_full_name, customer_email, customer_gender, customer_region,
customer_signup_date
FROM sub
WHERE rn=1
