{{ config(materialized='view') }}

with trip as (select * from {{ ref('stg_trips') }}),
trip_duration as (select driver_id, trip_id, rider_id, 
                    requested_at, pickup_at, dropoff_at, trip_status,
                    case when pickup_at is not null
                    and dropoff_at is not null
                    and dropoff_at >= pickup_at
                    and trip_status = 'completed'  # we can also consider counting trip duration for cancelled trips if they were cancelled after pickup, but for now we will only count completed trips
                    then timestamp_diff(dropoff_at, pickup_at, minute)
            else null
        end as trip_duration_minutes

    from trip

)

select *
from trip_duration