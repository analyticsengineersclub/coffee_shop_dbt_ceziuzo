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
         order_items.order_items_id,
         order_items.order_id,
         order_items.product_id,
         orders.customer_id,
         orders.state,
         products.product_name,
         products.product_category,
         product_prices.price,
         orders.sold_at,
         orders.customer_category

FROM order_items

LEFT JOIN orders
    ON order_items.order_id = orders.order_id

LEFT JOIN products
    ON order_items.product_id = products.product_id

LEFT JOIN product_prices
    ON order_items.product_id = product_prices.product_id
    AND orders.sold_at between product_prices.created_at and product_prices.ended_at
)

SELECT * FROM final