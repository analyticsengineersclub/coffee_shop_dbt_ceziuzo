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
        ranking=1 as is_new_customer

    from base

)

select * from renamed