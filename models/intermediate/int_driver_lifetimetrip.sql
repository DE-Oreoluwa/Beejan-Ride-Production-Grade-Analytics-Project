{{ config(materialized='view') }}

with trip as (select * from {{ ref('stg_trips') }}),
driver as (select * from {{ ref('stg_drivers') }}),

driver_trip_counts as (select d.driver_id, d.driver_status,
        count(t.trip_id) as driver_lifetimetrip_count,        
                    from driver d
                    left join trip t on d.driver_id = t.driver_id        
                                        # can as well add a where clause to filter only completed trips if we want to count only completed trips (where trip_status = 'completed')
                    group by d.driver_id, d.driver_status)

select * from driver_trip_counts