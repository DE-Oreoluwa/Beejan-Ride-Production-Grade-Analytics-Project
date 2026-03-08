# This snapshot captures the state of the drivers dimension at a specific point in time. It uses timestamp for updated_at column and driver_id to know which record to update when there is a change in the source data.
  {%
{% snapshot snapshot_drivers %} 
    set config = {
      'strategy': 'timestamp', 
      'unique_key': 'driver_id',
      'updated_at': 'updated_at'
    }
  %} 
  
  select
      driver_id,
      driver_status,
      vehicle_id,
      rating,
  from {{ ref('stg_drivers') }}

{% endsnapshot %}