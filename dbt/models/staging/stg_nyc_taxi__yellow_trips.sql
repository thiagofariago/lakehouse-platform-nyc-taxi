{{
    config(
        materialized='incremental',
        unique_key=['trip_id'],
        on_schema_change='append_new_columns',
        incremental_strategy='delete+insert',
        tags=['staging', 'nyc_taxi', 'yellow']
    )
}}

with source as (
    select * from {{ source('raw', 'yellow_trips') }}
    {% if is_incremental() %}
        where year > (select max(year) from {{ this }})
           or (year = (select max(year) from {{ this }})
               and month >= (select max(month) from {{ this }} where year = (select max(year) from {{ this }})))
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'tpep_pickup_datetime',
            'pulocationid',
            'dolocationid'
        ]) }} as trip_id,

        cast(vendorid as bigint) as vendor_id,
        cast(pulocationid as bigint) as pickup_location_id,
        cast(dolocationid as bigint) as dropoff_location_id,
        cast(tpep_pickup_datetime as timestamp(6)) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp(6)) as dropoff_datetime,
        cast(passenger_count as double) as passenger_count,
        cast(trip_distance as double) as trip_distance_miles,
        cast(fare_amount as double) as fare_amount,
        cast(total_amount as double) as total_amount,
        cast(year as integer) as year,
        cast(month as integer) as month,
        'yellow' as taxi_type,
        current_timestamp as loaded_at

    from source
    where tpep_pickup_datetime is not null
      and tpep_dropoff_datetime is not null
      and tpep_pickup_datetime < tpep_dropoff_datetime
      and trip_distance >= 0
      and total_amount >= 0
)

select * from renamed
