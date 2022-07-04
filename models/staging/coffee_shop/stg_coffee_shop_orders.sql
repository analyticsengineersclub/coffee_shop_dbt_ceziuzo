with source as (

    select * from {{ source('coffee_shop', 'orders') }}

),

base as (
    select *,
    rank() 
          over(partition by customer_id order by created_at asc) 
                 as ranking
    from source
    
),

renamed as (

    select
        id as order_id,
        customer_id,
        total,
        address,
        state,
        zip,
        created_at as sold_at,
        case when ranking = 1 then 'new_customer' 
               else 'returning_customer'
                end as customer_category

    from base

)

select * from renamed