{{ config(
    materialized='table'
) }}


with all_sessions as (
    select * from {{ ref('sessionization') }}
),


session_duration as (
    select distinct session_id,
           min(timestamp) over (partition by session_id) as start_at, 
           max(timestamp) over (partition by session_id) as end_at
           
    from all_sessions
),


num_pages_visited as (
    select
        session_id,
        count(visited_page) as number_of_pages
    from all_sessions
    group by session_id
),


session_purchase as (
    select distinct session_id,
          visited_page
    from all_sessions
    where visited_page like "%order-confirmation%"
)


select
     session_duration.session_id,
     start_at,
     end_at,
     date_diff(end_at, start_at, second) as duration_in_secs,
     number_of_pages as total_visited_pages,
     case when visited_page is null then 'no purchase made'
          else 'purchase made' end as purchase_category
from session_duration
join num_pages_visited
    using (session_id) 
left join session_purchase
    using (session_id)
    