{{ config(
    materialized='table'
) }}



WITH

order_items AS (
    SELECT * FROM {{ ref('stg_coffee_shop_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_coffee_shop_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_coffee_shop_products') }}
),

product_prices AS (
    SELECT * FROM {{ ref('stg_coffee_shop_product_prices') }}
),

final AS (
    SELECT
         date_trunc(date(orders.sold_at), week) AS weekly,
         products.product_category AS product_category,
         sum(product_prices.price) AS revenue

FROM order_items

LEFT JOIN orders
    ON order_items.order_id = orders.order_id

LEFT JOIN products
    ON order_items.product_id = products.product_id

LEFT JOIN product_prices
    ON order_items.product_id = product_prices.product_id

GROUP BY 1,2
)

SELECT * FROM final