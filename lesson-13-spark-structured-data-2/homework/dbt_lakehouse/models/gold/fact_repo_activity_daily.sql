-- Крок 10: gold.fact_repo_activity_daily. Специфікація: ../../SPEC.md → «Крок 10».

with commits_daily as (

    select
        repo_name,
        date(pushed_at) as day,
        count(*)                     as commits,
        count(distinct author_email) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('commits') }}
    group by repo_name, date(pushed_at)

),

prs_opened_daily as (

    select
        repo_name,
        date(opened_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        count(*)          as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('pull_requests') }}
    group by repo_name, date(opened_at)

),

prs_merged_daily as (

    select
        repo_name,
        date(merged_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        count(*)          as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('pull_requests') }}
    where merged_at is not null
    group by repo_name, date(merged_at)

),

issues_opened_daily as (

    select
        repo_name,
        date(opened_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        count(*)          as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('issues') }}
    group by repo_name, date(opened_at)

),

issues_closed_daily as (

    select
        repo_name,
        date(closed_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        count(*)          as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('issues') }}
    where closed_at is not null
    group by repo_name, date(closed_at)

),

stars_forks_daily as (

    select
        repo_name,
        date(created_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        sum(case when event_type = 'WatchEvent' then 1 else 0 end) as stars,
        sum(case when event_type = 'ForkEvent' then 1 else 0 end)  as forks
    from {{ ref('events') }}
    where event_type in ('WatchEvent', 'ForkEvent')
    group by repo_name, date(created_at)

),

unioned as (

    select * from commits_daily
    union all
    select * from prs_opened_daily
    union all
    select * from prs_merged_daily
    union all
    select * from issues_opened_daily
    union all
    select * from issues_closed_daily
    union all
    select * from stars_forks_daily

),

rolled_up as (

    select
        repo_name,
        day,
        sum(commits)             as commits,
        sum(distinct_committers) as distinct_committers,
        sum(prs_opened)          as prs_opened,
        sum(prs_merged)          as prs_merged,
        sum(issues_opened)       as issues_opened,
        sum(issues_closed)       as issues_closed,
        sum(stars)               as stars,
        sum(forks)               as forks
    from unioned
    group by repo_name, day

),

keyed as (

    select
        md5(repo_name)                            as repo_id,
        cast(date_format(day, 'yyyyMMdd') as int)  as date_id,
        commits,
        distinct_committers,
        prs_opened,
        prs_merged,
        issues_opened,
        issues_closed,
        stars,
        forks
    from rolled_up

)

select
    md5(concat_ws('|', repo_id, date_id)) as activity_id,
    repo_id,
    date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks
from keyed