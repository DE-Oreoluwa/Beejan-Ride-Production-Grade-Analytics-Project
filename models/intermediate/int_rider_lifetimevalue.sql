{{config(materialized='view') }}

with trips as (select *
            from {{ ref('stg_trips') }}),

payments as (select *
            from {{ ref('stg_payments') }}),

rider_payments as (select r.rider_id as rider_id, p.amount as amount
                    from trips r
                    left join payments p
                    on r.trip_id = p.trip_id
                    where r.trip_status = 'completed' and p.payment_status = 'success'
                    ), 

rider_lifetimevalue as (select rider_id, sum(amount) as rider_lifetime_value
                        from rider_payments
                        group by rider_id)

select * from rider_lifetimevalue

