{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='driver_key',
    )
}}

-- gold.dim_driver. Grain: водій. SCD Type 1 (поточні значення). SPEC.md, розділ 4.4.
--   * джерело — події ride_accepted
--   * «останній» рейтинг/авто — за occurred_at, а не за порядком прибуття
--   * водія перераховуємо з УСІЄЇ його історії: інакше запізніла подія зі старим часом
--     затерла б новіше значення

with accepted as (

    select
        payload #>> '{driver,id}'                              as driver_key,
        payload #>> '{driver,vehicle,type}'                    as vehicle_type,
        payload #>> '{driver,vehicle,medallion}'               as medallion,
        (payload #>> '{driver,rating}')::numeric(3, 2)         as rating,
        occurred_at,
        event_id,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'

),

{% if is_incremental() %}
touched as (

    select distinct driver_key
    from accepted
    where _ingested_at > {{ high_watermark() }}

),
{% endif %}

scoped as (

    select a.*
    from accepted as a
    {% if is_incremental() %}
    inner join touched as t on t.driver_key = a.driver_key
    {% endif %}

),

drivers as (

    select
        driver_key,
        (array_agg(vehicle_type order by occurred_at desc, event_id desc))[1] as vehicle_type,
        (array_agg(medallion    order by occurred_at desc, event_id desc))[1] as medallion,
        (array_agg(rating       order by occurred_at desc, event_id desc))[1] as latest_rating,
        min(occurred_at)                                                       as first_seen_at,
        max(occurred_at)                                                       as last_seen_at,
        max(_ingested_at)                                                      as _ingested_at
    from scoped
    where driver_key is not null
    group by driver_key

)

select
    driver_key,
    vehicle_type,
    medallion,
    latest_rating,
    first_seen_at,
    last_seen_at,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from drivers

union all

-- Член «unknown» для поїздок, скасованих до прийняття. unique_key = driver_key, тож
-- delete+insert замінює його при кожному запуску: після будь-якої кількості запусків він один.
-- _ingested_at = -infinity, щоб не зсувати межу інкременту (high_watermark бере max).
select
    'unknown'                      as driver_key,
    'unknown'                      as vehicle_type,
    null::text                     as medallion,
    null::numeric(3, 2)            as latest_rating,
    null::timestamptz              as first_seen_at,
    null::timestamptz              as last_seen_at,
    '-infinity'::timestamptz       as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
