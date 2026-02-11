{{
    config(
        materialized='view',
        tags=['intermediate', 'nyc_taxi', 'quality']
    )
}}

-- Filters high-quality trips only, removes outliers

with enriched_trips as (
    select * from {{ ref('int_trips_enriched') }}
),

cleaned as (
    select *
    from enriched_trips
    where is_high_quality_trip = true
        and avg_speed_mph < 80
            or avg_speed_mph is null
)

select * from cleaned
