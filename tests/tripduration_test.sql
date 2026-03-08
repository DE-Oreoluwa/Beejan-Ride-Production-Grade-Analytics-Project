with tripduration_test as (select * from {{ ref('int_tripdura_mins') }})

select * from tripduration_test
where trip_duration_minutes <= 0