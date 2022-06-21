{{  config(
       materialized='table'
    )
}}


WITH customers_and_orders AS (
  SELECT  customer_id,
          name, 
          email, 
          created_at AS first_time_order, 
          rank() over(partition by customer_id order by created_at asc) as ranking_orders,
          count(*) over(partition by customer_id) AS number_of_orders
FROM {{ source('coffee_shop', 'orders') }} a
LEFT JOIN {{ source('coffee_shop', 'customers') }} b on a.customer_id = b.id
GROUP BY 1,2,3,4
QUALIFY ranking_orders = 1
)

SELECT customer_id, 
       name, 
       email, 
       first_time_order, 
       number_of_orders
FROM customers_and_orders
ORDER BY first_time_order