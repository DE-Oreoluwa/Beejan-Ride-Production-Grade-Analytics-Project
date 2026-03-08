{% macro failed_payment_on_completed_trip(trip_status, payment_status) %}
case when {{ trip_status }} = 'completed'
    and {{ payment_status }} = 'failed'
    then 1 else 0
end
{% endmacro %}

{% macro duplicate_trip_payments(payment_id, trip_id) %}
case
    when count({{ payment_id }}) over (partition by {{ trip_id }}) > 1
    then 1 else 0
end
{% endmacro %}