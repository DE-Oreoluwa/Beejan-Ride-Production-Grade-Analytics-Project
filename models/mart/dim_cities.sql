{{config(materialized='table') }}

with cities as (select * from {{ ref('stg_cities') }}),
trips as (select * from {{ ref('stg_trips') }}),
revenue as (select * from {{ ref('stg_payments') }})

select c.city_id, c.city_name, c.country, cast(r.created_at as date) as created_date, r.amount as city_revenue_by_day
from cities c
left join trips t on c.city_id = t.city_id
left join revenue r on t.trip_id = r.trip_id
where r.created_at is not null
order by created_date