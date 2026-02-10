{{
    config(
        materialized="view",
        tags=["intermediate", "nyc_taxi"]
    )
}}

/*
    Intermediate - Trips Unified
    
    Combina todos os tipos de taxi (yellow, green, fhv, fhvhv) em um schema unificado
*/

with yellow_trips as (
    select
        trip_id,
        'yellow' as taxi_type,
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance_miles,
        fare_amount,
        total_amount,
        cast(null as double) as base_passenger_fare,
        cast(null as varchar) as hvfhs_license_number,
        cast(null as varchar) as dispatching_base_number,
        year,
        month,
        loaded_at
    from {{ ref("stg_nyc_taxi__yellow_trips") }}
),

green_trips as (
    select
        trip_id,
        'green' as taxi_type,
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance_miles,
        fare_amount,
        total_amount,
        cast(null as double) as base_passenger_fare,
        cast(null as varchar) as hvfhs_license_number,
        cast(null as varchar) as dispatching_base_number,
        year,
        month,
        loaded_at
    from {{ ref("stg_nyc_taxi__green_trips") }}
),

fhv_trips as (
    select
        trip_id,
        'fhv' as taxi_type,
        cast(null as bigint) as vendor_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        cast(null as double) as passenger_count,
        cast(null as double) as trip_distance_miles,
        cast(null as double) as fare_amount,
        cast(null as double) as total_amount,
        cast(null as double) as base_passenger_fare,
        cast(null as varchar) as hvfhs_license_number,
        dispatching_base_number,
        year,
        month,
        loaded_at
    from {{ ref("stg_nyc_taxi__fhv_trips") }}
),

fhvhv_trips as (
    select
        trip_id,
        'fhvhv' as taxi_type,
        cast(null as bigint) as vendor_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        cast(null as double) as passenger_count,
        trip_distance_miles,
        cast(null as double) as fare_amount,
        cast(null as double) as total_amount,
        base_passenger_fare,
        hvfhs_license_number,
        dispatching_base_number,
        year,
        month,
        loaded_at
    from {{ ref("stg_nyc_taxi__fhvhv_trips") }}
),

unified as (
    select * from yellow_trips
    union all
    select * from green_trips
    union all
    select * from fhv_trips
    union all
    select * from fhvhv_trips
)

select * from unified
