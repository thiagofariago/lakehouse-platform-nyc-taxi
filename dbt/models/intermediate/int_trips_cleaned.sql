{{
    config(
        materialized='view',
        tags=['intermediate', 'nyc_taxi', 'quality']
    )
}}

/*
    Intermediate - Trips Cleaned

    Filtra apenas viagens de alta qualidade baseado nos flags do enriched
    Remove outliers e dados suspeitos
*/

with enriched_trips as (
    select * from {{ ref('int_trips_enriched') }}
),

cleaned as (
    select *
    from enriched_trips
    where is_high_quality_trip = true
        -- Filtros adicionais de qualidade
        and avg_speed_mph < 80  -- Velocidade máxima razoável em NYC (or null for FHV)
            or avg_speed_mph is null
)

select * from cleaned
