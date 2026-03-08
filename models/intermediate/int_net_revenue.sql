{{config(materialized='view')}}

with trip as (select * from {{ ref('stg_trips') }}),
payment as (select * from {{ref('stg_payments')}}),
trip_payment as (select t.trip_id as trip_id, t.rider_id as rider_id, t.driver_id as driver_id, 
                p.amount as amount, p.fee as fee, t.trip_status as trip_status,
                    case when t.trip_status = 'completed' 
                    and p.payment_status = 'success' then p.amount else 0 end as gross_revenue
                    from trip t
                    left join payment p on t.trip_id = p.trip_id),

net_revenue as (select driver_id, SUM(gross_revenue - fee) as net_revenue, SUM(gross_revenue) as total_gross_revenue
                from trip_payment
                where trip_status = 'completed' and gross_revenue > 0
                group by driver_id
                )
select * from net_revenue