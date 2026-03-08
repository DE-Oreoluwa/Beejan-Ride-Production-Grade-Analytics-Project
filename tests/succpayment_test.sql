with succpayment_test as (select * from {{ ref('int_fraud_indicators') }})

select *
from succpayment_test
where trip_status = 'cancelled'
and payment_status = 'failed'