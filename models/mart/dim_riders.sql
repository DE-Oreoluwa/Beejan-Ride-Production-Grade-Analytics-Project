{{config(materialized='table') }}

with rider as (select * from {{ ref('stg_riders') }}),
rider_lifetimevalue as (select * from {{ ref('int_rider_lifetimevalue') }}),
corporate_tripflag as (select * from {{ ref('int_corporate_tripflag') }})
select r.rider_id, r.signup_date, r.country, rl.rider_lifetime_value, c.is_corporate, c.corporate_trip_flagin
from rider r
left join rider_lifetimevalue rl on r.rider_id = rl.rider_id
left join corporate_tripflag c on r.rider_id = c.rider_id
