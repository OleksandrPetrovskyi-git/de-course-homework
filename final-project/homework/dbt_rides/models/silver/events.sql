{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='event_id',
    )
}}

-- silver.events. Grain: одна подія (event_id). SPEC.md, розділ 4.2.
--   * межа інкременту — _ingested_at (high_watermark), а НЕ occurred_at: запізніла подія має
--     старий occurred_at, але приїжджає в новому батчі
--   * дублікат, що вже є в Silver, пропускаємо: вміст події незмінний, лишається найраніший

with source_rows as (

    select
        b.event_id,
        b.event_type,
        b.ride_id,
        b.source,
        b.occurred_at,
        b.payload,
        b._source_file,
        b._ingested_at
    from {{ source('bronze', 'raw_events') }} as b
    where b.source is distinct from 'loadtest'
      and b.occurred_at is not null
      and b.ride_id is not null
    {% if is_incremental() %}
      and b._ingested_at > {{ high_watermark() }}
      and not exists (
          select 1 from {{ this }} as t where t.event_id = b.event_id
      )
    {% endif %}

),

ranked as (

    select
        *,
        row_number() over (
            partition by event_id
            order by _ingested_at, _source_file
        ) as rn
    from source_rows

)

select
    event_id,
    event_type,
    ride_id,
    source,
    occurred_at,
    {{ utc_date('occurred_at') }} as occurred_date,
    payload::jsonb as payload,
    _source_file,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from ranked
where rn = 1
