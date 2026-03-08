{{ config(materialized='view') }}

with source as (
    select * from {{ source('dbt_project_486613_beejan_dataset', 'trips_raw') }}
),

casted_values as (select
        cast(trip_id as string) as trip_id,
        cast(rider_id as string) as rider_id,
        cast(driver_id as string) as driver_id,
        cast(vehicle_id as string) as vehicle_id,
        cast(city_id as string) as city_id,
        cast(requested_at as timestamp) as requested_at,
        cast(pickup_at as timestamp) as pickup_at,
        cast(dropoff_at as timestamp) as dropoff_at,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        lower(trim(status)) as trip_status,
        lower(trim(payment_method)) as payment_method,
        cast(estimated_fare as numeric) as estimated_fare,
        cast(actual_fare as numeric) as actual_fare,
        cast(surge_multiplier as numeric) as surge_multiplier,
        cast(is_corporate as boolean) as is_corporate
    from source),

filtered as (select *
    from casted_values
    where trip_id is not null),

deduplicated as (select *
    from (select *,
            row_number() over (partition by trip_id order by updated_at desc) as row_num
        from filtered)
    where row_num = 1)

select
    trip_id,
    rider_id,
    driver_id,
    vehicle_id,
    city_id,
    requested_at,
    pickup_at,
    dropoff_at,
    trip_status,
    estimated_fare,
    actual_fare,
    surge_multiplier,
    payment_method,
    is_corporate,
    created_at,
    updated_at
from deduplicated