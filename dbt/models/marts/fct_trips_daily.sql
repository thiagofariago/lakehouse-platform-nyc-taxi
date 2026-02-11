{{
    config(
        materialized="table",
        tags=["marts", "facts", "aggregated"]
    )
}}

-- Daily aggregates: one row per (date, taxi_type)

with trips as (
    select * from {{ ref("fct_trips") }}
)

select
    date(pickup_datetime) as trip_date,
    taxi_type,
    year,
    month,

    count(*) as total_trips,
    count(distinct pickup_location_id) as unique_pickup_locations,
    count(distinct dropoff_location_id) as unique_dropoff_locations,

    round(sum(trip_distance_miles), 2) as total_distance_miles,
    round(avg(trip_distance_miles), 2) as avg_distance_miles,
    round(min(trip_distance_miles), 2) as min_distance_miles,
    round(max(trip_distance_miles), 2) as max_distance_miles,

    round(sum(trip_duration_minutes), 2) as total_duration_minutes,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
    round(min(trip_duration_minutes), 2) as min_duration_minutes,
    round(max(trip_duration_minutes), 2) as max_duration_minutes,

    round(avg(avg_speed_mph), 2) as avg_speed_mph,

    round(sum(total_amount), 2) as total_revenue,
    round(avg(total_amount), 2) as avg_fare,
    round(avg(cost_per_mile), 2) as avg_cost_per_mile,

    round(avg(passenger_count), 2) as avg_passengers,
    round(sum(passenger_count), 0) as total_passengers,

    sum(case when time_of_day = 'Morning' then 1 else 0 end) as trips_morning,
    sum(case when time_of_day = 'Afternoon' then 1 else 0 end) as trips_afternoon,
    sum(case when time_of_day = 'Evening' then 1 else 0 end) as trips_evening,
    sum(case when time_of_day = 'Night' then 1 else 0 end) as trips_night,

    current_timestamp as created_at

from trips
group by 1, 2, 3, 4
