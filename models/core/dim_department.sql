{{
    config(
        materialized='table'
    )
}}

SELECT *
FROM {{ ref('category_to_department') }}