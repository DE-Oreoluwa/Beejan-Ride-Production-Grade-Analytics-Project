{{ config(materialized='view') }}

with trips as (select * from {{ ref('stg_trips') }}),

driver_activity as (select driver_id,
                max(pickup_at) as last_trip_date,
                date_diff(current_date(), date(max(pickup_at)), day) as days_since_last_trip
            from trips
            where trip_status = 'completed'
            group by driver_id),

driver_churn as (select driver_id, last_trip_date, days_since_last_trip,
            case when days_since_last_trip > 30
            then 1 else 0 end as churn_flag
        from driver_activity)

select *
from driver_churn