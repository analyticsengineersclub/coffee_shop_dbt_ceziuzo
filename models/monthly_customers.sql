{{  config(
       materialized='table'
    )
}}


SELECT
    date_trunc(first_time_order, month) AS signup_month,
    count(*) AS new_customers

FROM {{  ref('customers')   }} 

GROUP BY 1