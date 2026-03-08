{{ config(materialized='view')}}

with source as (
    select * from {{ source('dbt_project_486613_beejan_dataset', 'cities_raw') }}
),

casted_values as (select cast(city_id as string) as city_id,
       cast(country as string) as country,
       cast(city_name as string) as city_name,
       cast(launch_date as date) as launch_date
        from source),

deduplicated as (select distinct * from casted_values 
            where city_id is not null)

select 
    city_id,
    country,
    city_name,
    launch_date
 from deduplicated

