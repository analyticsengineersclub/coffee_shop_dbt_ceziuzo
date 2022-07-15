{{ config(
    materialized='table'
) }}


with all_orders as (
    select * from {{ ref('all_orders') }}
),

final as (
      select 
           date_trunc(sold_at, month) as date_month,
           sum(case when product_category = 'coffee beans' then price end) as coffee_beans_amount,
           sum(case when product_category = 'merch' then price end) as merch_amount,
           sum(case when product_category = 'brewing supplies' then price end) as brewing_supplies_amount
 
     from all_orders
 
     group by 1
     order by 1
)

select * from final