{{
    config(
        materialized="table",
        tags=["marts", "facts", "aggregated"]
    )
}}

/*
    Fact Table - Monthly Trips Aggregated
    
    Métricas agregadas por mês e tipo de taxi.
    Otimizado para análises de tendências mensais.
    
    Grão: Uma linha por (year, month, taxi_type)
*/

with trips as (
    select * from {{ ref("fct_trips") }}
)

select
    -- Dimensões
    year,
    month,
    taxi_type,
    
    -- Data para facilitar joins
    date(cast(year as varchar) || '-' || lpad(cast(month as varchar), 2, '0') || '-01') as month_start_date,
    
    -- Contagens
    count(*) as total_trips,
    count(distinct pickup_location_id) as unique_pickup_locations,
    count(distinct dropoff_location_id) as unique_dropoff_locations,
    count(distinct date(pickup_datetime)) as days_with_trips,
    
    -- Métricas de distância
    round(sum(trip_distance_miles), 2) as total_distance_miles,
    round(avg(trip_distance_miles), 2) as avg_distance_miles,
    
    -- Métricas de duração
    round(sum(trip_duration_minutes) / 60.0, 2) as total_duration_hours,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
    
    -- Métricas de velocidade
    round(avg(avg_speed_mph), 2) as avg_speed_mph,
    
    -- Métricas financeiras (apenas yellow/green)
    round(sum(total_amount), 2) as total_revenue,
    round(avg(total_amount), 2) as avg_fare,
    round(sum(total_amount) / nullif(count(*), 0), 2) as revenue_per_trip,
    
    -- Passageiros (apenas yellow/green)
    round(sum(passenger_count), 0) as total_passengers,
    round(avg(passenger_count), 2) as avg_passengers,
    
    -- Distribuição temporal
    round(avg(case when time_of_day = 'Morning' then 1.0 else 0.0 end) * 100, 1) as pct_morning,
    round(avg(case when time_of_day = 'Afternoon' then 1.0 else 0.0 end) * 100, 1) as pct_afternoon,
    round(avg(case when time_of_day = 'Evening' then 1.0 else 0.0 end) * 100, 1) as pct_evening,
    round(avg(case when time_of_day = 'Night' then 1.0 else 0.0 end) * 100, 1) as pct_night,
    
    -- Distribuição por dia da semana
    round(avg(case when pickup_day_of_week in (6, 7) then 1.0 else 0.0 end) * 100, 1) as pct_weekend,
    
    -- Metadata
    current_timestamp as created_at

from trips
group by 1, 2, 3
