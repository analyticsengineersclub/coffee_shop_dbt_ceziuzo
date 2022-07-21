{{ config(
    materialized='table'
) }}

{% set product_category = dbt_utils.get_column_values(table=ref('stg_coffee_shop_products'), column='product_category') %}

with all_orders as (
    select * from {{ ref('all_orders') }}
),

final as (
      select 
           date_trunc(sold_at, month) as date_month,

           {% for product_category in product_category %}
           sum(case when product_category = '{{product_category}}' then price end) as {{ product_category | replace(" ", "_")  }}_amount,
           {% endfor %}

      from all_orders
      group by 1
      order by 1
)

select * from final