{{
    config(
        materialized='view',
        tags=['intermediate', 'nyc_taxi']
    )
}}

-- Adds derived metrics: duration, speed, cost, temporal features, quality flags

with trips as (
    select * from {{ ref('int_trips_unified') }}
),

enriched as (
    select
        *,

        -- Trip duration
        date_diff('second', pickup_datetime, dropoff_datetime) as trip_duration_seconds,
        date_diff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes,
        round(date_diff('second', pickup_datetime, dropoff_datetime) / 3600.0, 2) as trip_duration_hours,

        -- Average speed (mph)
        case
            when date_diff('second', pickup_datetime, dropoff_datetime) > 0
                and trip_distance_miles > 0
            then round(
                trip_distance_miles / (date_diff('second', pickup_datetime, dropoff_datetime) / 3600.0),
                2
            )
            else null
        end as avg_speed_mph,

        -- Cost per mile (yellow/green only)
        case
            when trip_distance_miles > 0 and total_amount is not null
            then round(total_amount / trip_distance_miles, 2)
            else null
        end as cost_per_mile,

        -- Cost per minute
        case
            when date_diff('minute', pickup_datetime, dropoff_datetime) > 0 and total_amount is not null
            then round(total_amount / date_diff('minute', pickup_datetime, dropoff_datetime), 2)
            else null
        end as cost_per_minute,

        -- Temporal features
        hour(pickup_datetime) as pickup_hour,
        day_of_week(pickup_datetime) as pickup_day_of_week,
        case day_of_week(pickup_datetime)
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
            when 7 then 'Sunday'
        end as pickup_day_name,

        case
            when hour(pickup_datetime) between 6 and 11 then 'Morning'
            when hour(pickup_datetime) between 12 and 17 then 'Afternoon'
            when hour(pickup_datetime) between 18 and 22 then 'Evening'
            else 'Night'
        end as time_of_day,

        -- Quality flags
        case
            when date_diff('minute', pickup_datetime, dropoff_datetime) between 1 and 180
            then true else false
        end as is_valid_duration,

        case
            when trip_distance_miles between 0.1 and 100
            then true else false
        end as is_valid_distance,

        case
            when total_amount between 0 and 500
                or total_amount is null
            then true else false
        end as is_valid_fare,

        case
            when date_diff('minute', pickup_datetime, dropoff_datetime) between 1 and 180
                and (trip_distance_miles between 0.1 and 100 or trip_distance_miles is null)
                and (total_amount between 0 and 500 or total_amount is null)
                and pickup_location_id is not null
                and dropoff_location_id is not null
            then true
            else false
        end as is_high_quality_trip

    from trips
)

select * from enriched
