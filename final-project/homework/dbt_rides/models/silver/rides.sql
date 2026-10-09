{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='ride_id',
    )
}}

-- silver.rides. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
--   * «зачеплені» поїздки = ті, у яких є події новіші за межу інкременту
--   * зачеплену поїздку перебудовуємо з УСІЄЇ її історії в events, а не лише з нового батча:
--     запізніле payment_captured має дописати paid_at до поїздки, збудованої раніше
--   * поїздка існує, коли прийшла її ride_requested (інакше події чекають у silver.events)

with touched as (

    select distinct ride_id
    from {{ ref('events') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}

),

ev as (

    select e.*
    from {{ ref('events') }} as e
    inner join touched on touched.ride_id = e.ride_id

),

-- по одній (найранішій) події кожного типу на поїздку: порядок прибуття не має значення
first_of as (

    select
        ride_id,
        event_type,
        (array_agg(payload order by occurred_at, event_id))[1] as payload,
        min(occurred_at) as occurred_at
    from ev
    group by ride_id, event_type

),

ingested as (

    select ride_id, max(_ingested_at) as _ingested_at
    from ev
    group by ride_id

)

select
    req.ride_id,
    req.payload #>> '{rider,id}'                       as rider_id,
    req.payload #>> '{rider,platform}'                 as rider_platform,
    req.payload #>> '{rider,app_version}'              as app_version,
    req.payload ->> 'requested_vehicle'                as requested_vehicle,
    coalesce(
        acc.payload #>> '{driver,id}',
        sta.payload #>> '{driver,id}',
        com.payload #>> '{driver,id}'
    )                                                  as driver_id,
    case
        when com.occurred_at is not null then 'completed'
        when can.occurred_at is not null then 'cancelled'
        when sta.occurred_at is not null then 'in_progress'
        when acc.occurred_at is not null then 'accepted'
        else 'requested'
    end                                                as status,
    req.occurred_at                                    as requested_at,
    acc.occurred_at                                    as accepted_at,
    sta.occurred_at                                    as started_at,
    com.occurred_at                                    as completed_at,
    can.occurred_at                                    as cancelled_at,
    pay.occurred_at                                    as paid_at,
    -- фактична зона (started / completed) перекриває заявлену (requested)
    coalesce(
        (sta.payload #>> '{pickup,zone_id}')::int,
        (req.payload #>> '{pickup,zone_id}')::int
    )                                                  as pickup_zone_id,
    coalesce(
        (com.payload #>> '{dropoff,zone_id}')::int,
        (req.payload #>> '{dropoff,zone_id}')::int
    )                                                  as dropoff_zone_id,
    round(extract(epoch from (acc.occurred_at - req.occurred_at)))::int as wait_seconds,
    round(extract(epoch from (com.occurred_at - sta.occurred_at)))::int as trip_seconds,
    (req.payload ->> 'surge_estimate')::numeric        as surge_estimate,
    (com.payload ->> 'distance_km')::numeric(8, 2)     as distance_km,
    -- множник береться ФАКТИЧНИЙ (ride_completed), а не оцінка із замовлення
    (com.payload #>> '{fare,surge_multiplier}')::numeric(4, 2) as surge_multiplier,
    (com.payload #>> '{fare,amount}')::numeric(10, 2)  as fare_amount,
    (com.payload #>> '{fare,tolls}')::numeric(10, 2)   as tolls_amount,
    -- JSON null -> SQL NULL: готівкові чайові лишаються NULL, а не 0
    (com.payload #>> '{fare,tip}')::numeric(10, 2)     as tip_amount,
    (com.payload #>> '{fare,total}')::numeric(10, 2)   as total_amount,
    com.payload #>> '{fare,currency}'                  as currency,
    can.payload ->> 'cancelled_by'                     as cancelled_by,
    can.payload ->> 'reason'                           as cancel_reason,
    can.payload ->> 'stage'                            as cancel_stage,
    pay.payload #>> '{payment,method}'                 as payment_method,
    pay.payload #>> '{payment,psp_reference}'          as psp_reference,
    (pay.payload #>> '{payment,amount}')::numeric(10, 2) as payment_amount,
    ing._ingested_at                                   as _ingested_at,
    '{{ run_started_at }}'::timestamptz                as _loaded_at
from first_of as req
left join first_of as acc on acc.ride_id = req.ride_id and acc.event_type = 'ride_accepted'
left join first_of as sta on sta.ride_id = req.ride_id and sta.event_type = 'ride_started'
left join first_of as com on com.ride_id = req.ride_id and com.event_type = 'ride_completed'
left join first_of as can on can.ride_id = req.ride_id and can.event_type = 'ride_cancelled'
left join first_of as pay on pay.ride_id = req.ride_id and pay.event_type = 'payment_captured'
inner join ingested as ing on ing.ride_id = req.ride_id
where req.event_type = 'ride_requested'
