{{ config(materialized='view')}}

with trip as (select * from {{ ref('stg_trips') }}),
corporate_tripflag as (select trip_id, rider_id, driver_id, city_id,
                        trip_status,pickup_at, dropoff_at, is_corporate,
                        case when is_corporate = true then 1 else 0 end as corporate_trip_flagin
    from trip) 

select trip_id, rider_id, driver_id, city_id,
trip_status,pickup_at, dropoff_at, is_corporate, corporate_trip_flagin
 from corporate_tripflag