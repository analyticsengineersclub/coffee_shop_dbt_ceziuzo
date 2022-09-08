{{ config(
    materialized='table')
}}


select
	current_date() as as_at,
    favorite_ice_cream_flavor,
    count(*) as total
from {{ source('advanced_dbt_examples', 'favorite_ice_cream_flavors') }}
group by 1,2
order by 3 desc