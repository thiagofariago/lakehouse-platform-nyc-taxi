{{
    config(
        materialized="table",
        tags=["marts", "facts"]
    )
}}

-- Main fact table: one row per high-quality trip

with cleaned_trips as (
    select * from {{ ref("int_trips_cleaned") }}
)

select
    trip_id,
    taxi_type,

    pickup_location_id,
    dropoff_location_id,

    pickup_datetime,
    dropoff_datetime,
    year,
    month,
    pickup_hour,
    pickup_day_of_week,
    pickup_day_name,
    time_of_day,

    trip_distance_miles,
    trip_duration_minutes,
    trip_duration_hours,
    avg_speed_mph,

    fare_amount,
    total_amount,
    cost_per_mile,
    cost_per_minute,

    base_passenger_fare,

    passenger_count,
    vendor_id,
    hvfhs_license_number,
    dispatching_base_number,

    is_valid_duration,
    is_valid_distance,
    is_valid_fare,
    is_high_quality_trip,

    loaded_at

from cleaned_trips
