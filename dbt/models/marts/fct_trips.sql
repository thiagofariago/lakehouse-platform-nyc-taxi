{{
    config(
        materialized="table",
        tags=["marts", "facts"]
    )
}}

/*
    Fact Table - Trips
    
    Tabela de fatos principal com todas as viagens de alta qualidade.
    Base para dashboards e análises.
    
    Grão: Uma linha por viagem
*/

with cleaned_trips as (
    select * from {{ ref("int_trips_cleaned") }}
)

select
    -- IDs
    trip_id,
    taxi_type,
    
    -- Localizações
    pickup_location_id,
    dropoff_location_id,
    
    -- Temporal
    pickup_datetime,
    dropoff_datetime,
    year,
    month,
    pickup_hour,
    pickup_day_of_week,
    pickup_day_name,
    time_of_day,
    
    -- Métricas de viagem
    trip_distance_miles,
    trip_duration_minutes,
    trip_duration_hours,
    avg_speed_mph,
    
    -- Métricas financeiras (apenas yellow/green)
    fare_amount,
    total_amount,
    cost_per_mile,
    cost_per_minute,
    
    -- Métricas FHVHV (Uber/Lyft)
    base_passenger_fare,
    
    -- Outros
    passenger_count,
    vendor_id,
    hvfhs_license_number,
    dispatching_base_number,
    
    -- Flags de qualidade
    is_valid_duration,
    is_valid_distance,
    is_valid_fare,
    is_high_quality_trip,
    
    -- Metadata
    loaded_at

from cleaned_trips
