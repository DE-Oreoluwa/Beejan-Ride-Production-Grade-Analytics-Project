
with revenue_test as (select * from {{ ref('fact_trips') }})

select * from revenue_test
where net_revenue < 0
