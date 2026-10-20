{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='requested_hour',
    )
}}

-- gold.agg_zone_hourly. Grain: (requested_hour, pickup_zone_key). SPEC.md, розділ 4.4.
--
-- Ключ інкременту — requested_hour, а не (hour, zone). Поїздка може змінити зону посадки
-- (ride_started приїхав пізніше за ride_requested): тоді вона переїжджає з бакета
-- (година, стара зона) в (година, нова зона). Година поїздки не змінюється ніколи, тож
-- перераховуємо ЦІЛІ години, яких торкнулися змінені поїздки, і delete+insert видаляє всі
-- старі рядки цих годин (разом із застарілим бакетом старої зони, який міг стати порожнім).

with touched_hours as (

    select distinct requested_hour
    from {{ ref('fact_ride') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}

),

rides_in_hours as (

    select f.*
    from {{ ref('fact_ride') }} as f
    inner join touched_hours as h on h.requested_hour = f.requested_hour

)

select
    requested_hour,
    pickup_zone_key,
    count(*)                                                                    as rides_requested,
    count(*) filter (where status = 'completed')                                as rides_completed,
    count(*) filter (where status = 'cancelled')                                as rides_cancelled,
    coalesce(sum(total_amount) filter (where status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
    coalesce(sum(tip_amount)   filter (where status = 'completed'), 0)::numeric(12, 2) as tips,
    round(avg(wait_seconds), 2)::numeric(10, 2)                                 as avg_wait_seconds,
    max(_ingested_at)                                                           as _ingested_at,
    '{{ run_started_at }}'::timestamptz                                         as _loaded_at
from rides_in_hours
group by requested_hour, pickup_zone_key
