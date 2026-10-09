{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='ride_id',
    )
}}

-- gold.fact_ride. Grain: поїздка (accumulating snapshot). SPEC.md, розділ 4.4.
--   * silver.rides.`_ingested_at` = max по всіх подіях поїздки, тож будь-яка нова подія
--     підіймає його вище за межу, і факт підхоплює зміну (нова віха, запізніла оплата)
--   * ключ, якого нема у виміру, складається в член -1 / 'unknown', а не губить рядок

with changed as (

    select *
    from {{ ref('rides') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}

)

select
    r.ride_id,
    coalesce(pz.zone_key, -1)               as pickup_zone_key,
    coalesce(dz.zone_key, -1)               as dropoff_zone_key,
    coalesce(d.driver_key, 'unknown')       as driver_key,
    r.rider_id,
    r.status,
    r.requested_at,
    r.accepted_at,
    r.started_at,
    r.completed_at,
    r.cancelled_at,
    r.paid_at,
    {{ utc_date('r.requested_at') }}        as requested_date,
    -- початок години за UTC (без явного UTC межі години залежали б від пояса сесії)
    date_trunc('hour', r.requested_at at time zone 'UTC') at time zone 'UTC' as requested_hour,
    r.wait_seconds,
    r.trip_seconds,
    r.distance_km,
    r.fare_amount,
    r.surge_multiplier,
    r.tolls_amount,
    r.tip_amount,
    r.total_amount,
    r.currency,
    r.payment_method,
    r.payment_amount,
    r.cancelled_by,
    r.cancel_reason,
    r.cancel_stage,
    r._ingested_at,
    '{{ run_started_at }}'::timestamptz     as _loaded_at
from changed as r
left join {{ ref('dim_zone') }}   as pz on pz.zone_key = r.pickup_zone_id
left join {{ ref('dim_zone') }}   as dz on dz.zone_key = r.dropoff_zone_id
left join {{ ref('dim_driver') }} as d  on d.driver_key = r.driver_id
