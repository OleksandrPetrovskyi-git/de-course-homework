-- Крок 7: gold.dim_date. Специфікація: ../../SPEC.md → «Крок 7».

with dates as (

    select date(pushed_at) as d from {{ ref('commits') }}
    union all
    select date(opened_at) as d from {{ ref('pull_requests') }}
    union all
    select date(merged_at) as d from {{ ref('pull_requests') }} where merged_at is not null
    union all
    select date(opened_at) as d from {{ ref('issues') }}
    union all
    select date(closed_at) as d from {{ ref('issues') }} where closed_at is not null

),

bounds as (

    select min(d) as min_date, max(d) as max_date
    from dates

),

calendar as (

    select explode(sequence(min_date, max_date, interval 1 day)) as date_day
    from bounds

)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_id,
    date_day,
    dayofweek(date_day)              as day_of_week,
    dayofweek(date_day) in (1, 7)    as is_weekend,
    weekofyear(date_day)             as iso_week,
    year(date_day)                   as year
from calendar