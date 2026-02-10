{{
    config(
        materialized="incremental",
        unique_key=["trip_id"],
        on_schema_change="append_new_columns",
        incremental_strategy="delete+insert",
        tags=["staging", "nyc_taxi", "fhv"]
    )
}}

with source as (
    select * from {{ source("raw", "fhv_trips") }}
    {% if is_incremental() %}
        where year > (select max(year) from {{ this }})
           or (year = (select max(year) from {{ this }})
               and month >= (select max(month) from {{ this }} where year = (select max(year) from {{ this }})))
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "pickup_datetime",
            "pulocationid",
            "dolocationid"
        ]) }} as trip_id,

        cast(pulocationid as bigint) as pickup_location_id,
        cast(dolocationid as bigint) as dropoff_location_id,
        cast(pickup_datetime as timestamp(6)) as pickup_datetime,
        cast(dropoff_datetime as timestamp(6)) as dropoff_datetime,
        cast(dispatching_base_num as varchar) as dispatching_base_number,
        cast(year as integer) as year,
        cast(month as integer) as month,
        'fhv' as taxi_type,
        current_timestamp as loaded_at

    from source
    where pickup_datetime is not null
      and dropoff_datetime is not null
      and pickup_datetime < dropoff_datetime
)

select * from renamed
