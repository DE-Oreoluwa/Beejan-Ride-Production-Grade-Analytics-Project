{{ config(materialized='view') }}

with source as (
    select * from {{ source('dbt_project_486613_beejan_dataset', 'payments_raw') }}
),

casted_values as (select cast(payment_id as string) as payment_id,
        cast(trip_id as string) as trip_id,
        lower(trim(payment_status)) as payment_status,
        lower(trim(payment_provider)) as payment_provider,
        cast(amount as numeric) as amount,
        cast(fee as numeric) as fee,
        upper(trim(currency)) as currency,
        cast(created_at as timestamp) as created_at
    from source),

filtered as (select * from casted_values
            where payment_id is not null),

deduplicated as (select *
    from (select *, row_number() over (partition by payment_id 
            order by created_at desc) as row_num
            from filtered)
    where row_num = 1
    )

select
    payment_id,
    trip_id,
    payment_status,
    payment_provider,
    amount,
    fee,
    currency,
    created_at
from deduplicated