{{ config(materialized='view') }}

with source as (
    select * from {{ source('dbt_project_486613_beejan_dataset', 'riders_raw') }}
),

casted_values as (select cast(rider_id as string) as rider_id,
        cast(country as string) as country,
        cast(referral_code as string) as referral_code,
        cast(signup_date as date) as signup_date,
        cast(created_at as timestamp) as created_at,
        from source),

filtered as (
    select *
    from casted_values
    where rider_id is not null
),

deduplicated as (select *
    from (select *,
            row_number() over (partition by rider_id order by created_at desc) as row_num
        from filtered)
    where row_num = 1)

select
    rider_id,
    country,
    signup_date,
    referral_code,
    created_at
from deduplicated