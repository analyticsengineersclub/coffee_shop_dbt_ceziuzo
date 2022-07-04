{{ config(
    materialized='table'
) }}



WITH

all_orders AS (
    SELECT * FROM {{ ref('all_orders') }}
),

final AS (
    SELECT
        date_trunc(date(sold_at), week) AS weekly,
        customer_category,
        sum(price) AS revenue

    FROM all_orders
    
    GROUP BY 1,2

)

SELECT * FROM final