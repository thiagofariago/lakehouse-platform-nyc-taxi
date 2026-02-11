{{
    config(
        materialized="table",
        tags=["marts", "facts", "aggregated"]
    )
}}

-- Monthly aggregates: one row per (year, month, taxi_type)

with trips as (
    select * from {{ ref("fct_trips") }}
)

select
    year,
    month,
    taxi_type,

    date(cast(year as varchar) || '-' || lpad(cast(month as varchar), 2, '0') || '-01') as month_start_date,

    count(*) as total_trips,
    count(distinct pickup_location_id) as unique_pickup_locations,
    count(distinct dropoff_location_id) as unique_dropoff_locations,
    count(distinct date(pickup_datetime)) as days_with_trips,

    round(sum(trip_distance_miles), 2) as total_distance_miles,
    round(avg(trip_distance_miles), 2) as avg_distance_miles,

    round(sum(trip_duration_minutes) / 60.0, 2) as total_duration_hours,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes,

    round(avg(avg_speed_mph), 2) as avg_speed_mph,

    round(sum(total_amount), 2) as total_revenue,
    round(avg(total_amount), 2) as avg_fare,
    round(sum(total_amount) / nullif(count(*), 0), 2) as revenue_per_trip,

    round(sum(passenger_count), 0) as total_passengers,
    round(avg(passenger_count), 2) as avg_passengers,

    round(avg(case when time_of_day = 'Morning' then 1.0 else 0.0 end) * 100, 1) as pct_morning,
    round(avg(case when time_of_day = 'Afternoon' then 1.0 else 0.0 end) * 100, 1) as pct_afternoon,
    round(avg(case when time_of_day = 'Evening' then 1.0 else 0.0 end) * 100, 1) as pct_evening,
    round(avg(case when time_of_day = 'Night' then 1.0 else 0.0 end) * 100, 1) as pct_night,

    round(avg(case when pickup_day_of_week in (6, 7) then 1.0 else 0.0 end) * 100, 1) as pct_weekend,

    current_timestamp as created_at

from trips
group by 1, 2, 3
