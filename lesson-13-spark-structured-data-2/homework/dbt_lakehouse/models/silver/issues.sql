-- Крок 4: silver.issues. Специфікація: ../../SPEC.md → «Крок 4».

with issue_events as (

    select
        event_id,
        event_type,
        repo_name,
        created_at as event_at,
        from_json(payload, '{{ var("issue_schema") }}') as ev
    from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')

),

parsed as (

    select
        event_id,
        event_type,
        repo_name,
        event_at,
        ev.issue.number              as issue_number,
        ev.issue.title                as title,
        ev.issue.user.login           as author_login,
        ev.issue.state                as state,
        to_timestamp(ev.issue.created_at) as opened_at,
        to_timestamp(ev.issue.closed_at)  as closed_at,
        ev.issue.comments             as comments,
        transform(ev.issue.labels, l -> l.name) as label_names
    from issue_events

),

comment_counts as (

    select
        repo_name,
        issue_number,
        count(*) as comment_events_seen
    from parsed
    where event_type = 'IssueCommentEvent'
    group by repo_name, issue_number

),

ranked as (

    select
        *,
        row_number() over (
            partition by repo_name, issue_number
            order by event_at desc, event_id desc
        ) as rn
    from parsed

),

latest as (

    select * from ranked where rn = 1

)

select
    latest.repo_name,
    latest.issue_number,
    latest.title,
    latest.author_login,
    latest.state,
    latest.opened_at,
    latest.closed_at,
    latest.comments,
    latest.label_names,
    coalesce(cc.comment_events_seen, 0) as comment_events_seen,
    latest.event_at                     as last_event_at,
    case
        when latest.closed_at is not null
        then (unix_timestamp(latest.closed_at) - unix_timestamp(latest.opened_at)) / 3600.0
        else null
    end as hours_to_close
from latest
left join comment_counts cc
    on latest.repo_name = cc.repo_name
   and latest.issue_number = cc.issue_number