{{config(materialized='table') }}

with driver as (select * from {{ ref('stg_drivers') }}),
driver_churn as (select * from {{ ref('int_driver_churn') }}),
driver_lifetimetrip as (select * from {{ ref('int_driver_lifetimetrip') }}), 
revenue as (select * from {{ ref('int_net_revenue') }})

select d.driver_id, d.driver_status, dc.last_trip_date,
        dlt.driver_lifetimetrip_count, dc.churn_flag, r.total_gross_revenue, r.net_revenue, d.rating
from driver d
left join driver_churn dc on d.driver_id = dc.driver_id
left join driver_lifetimetrip dlt on d.driver_id = dlt.driver_id
left join revenue r on d.driver_id = r.driver_id
order by r.net_revenue desc
