{{ config(materialized='view')}}

with source as (
    select * from {{ source('dbt_project_486613_beejan_dataset', 'driver_status_events_raw') }}
),

casted_values as (select cast(event_id as string) as event_id,
       cast(driver_id as string) as driver_id,
       cast(status as string) as status,
       cast(event_timestamp as timestamp) as event_timestamp
        from source),

deduplicated as (select distinct * from casted_values 
            where event_id is not null)

select 
    event_id,
    driver_id,
    status,
    event_timestamp
 from deduplicated