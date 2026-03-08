{{config(materialized='view') }}

with trips as (select * from {{ref('stg_trips')}}),

payment as (select * from {{ref('stg_payments')}}),

trip_payment as (select t.trip_id, t.rider_id, t.driver_id, 
                p.amount, p.fee, t.trip_status, 
                t.surge_multiplier, p.payment_id, p.payment_status
                    from trips t
                    left join payment p on t.trip_id = p.trip_id),

fraud_indicators as (
        select *,
            {{ failed_payment_on_completed_trip('trip_status','payment_status') }} as failed_payment_on_completed_trip,
            {{ duplicate_trip_payments('payment_id','trip_id') }} as duplicate_trip_payments,
            case 
                when surge_multiplier > 10
                then 1
                else 0
            end as extreme_surge_multiplier 
        from trip_payment
                )

select * from fraud_indicators

