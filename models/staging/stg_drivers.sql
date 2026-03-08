{{ config(materialized='view') }}

with source as (
    select * from {{ source('dbt_project_486613_beejan_dataset', 'drivers_raw') }}
    ),

casted_values as (select cast(driver_id as string) as driver_id,
        cast(city_id as string) as city_id,
        cast(vehicle_id as string) as vehicle_id,
        cast(onboarding_date as date) as onboarding_date,
        lower(trim(driver_status)) as driver_status,
        cast(rating as numeric) as rating,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
        from source),

filtered as (select distinct * from casted_values 
            where driver_id is not null)

select 
    driver_id,
    city_id,
    vehicle_id,
    onboarding_date,
    driver_status,
    rating,
    created_at,
    updated_at
 from filtered