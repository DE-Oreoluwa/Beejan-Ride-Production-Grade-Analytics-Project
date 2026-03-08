{{ config(materialized= 'table') }}    #I used incremental materialization initially but failed to fun the second time I made changes to this model. I could not use it more than once in big query due to billing

with trip as (select * from {{ ref('stg_trips') }}),
city as (select * from {{ ref('stg_cities') }}),
duration_in_minutes as (select * from {{ ref('int_tripdura_mins')}}),
revenue as (select * from {{ ref('int_net_revenue') }}),
fraud_indicator as (select * from {{ ref('int_fraud_indicators') }}), 
corporate_tripflag as (select * from {{ ref('int_corporate_tripflag') }})

select t.trip_id, t.rider_id, t.driver_id, t.city_id, cz.city_name, 
        t.requested_at, t.pickup_at, t.dropoff_at, d.trip_duration_minutes,
        t.trip_status, t.payment_method, co.is_corporate, f.amount, f.fee, r.net_revenue, 
        f.failed_payment_on_completed_trip, f.duplicate_trip_payments, f.extreme_surge_multiplier,
        current_timestamp() as load_timestamp
from trip t
left join city cz on t.city_id = cz.city_id
left join duration_in_minutes d on t.trip_id = d.trip_id
left join revenue r on t.driver_id = r.driver_id
left join fraud_indicator f on t.trip_id = f.trip_id
left join corporate_tripflag co on t.trip_id = co.trip_id
order by requested_at
