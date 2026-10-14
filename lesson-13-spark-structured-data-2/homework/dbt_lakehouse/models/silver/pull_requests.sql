-- Крок 3: silver.pull_requests. Специфікація: ../../SPEC.md → «Крок 3».

with pr_events as (

    select
        event_id,
        repo_name,
        created_at as event_at,
        from_json(payload, '{{ var("pr_schema") }}') as pr
    from {{ ref('events') }}
    where event_type = 'PullRequestEvent'

),

parsed as (

    select
        event_id,
        repo_name,
        event_at,
        pr.action                        as action,
        pr.number                        as pr_number,
        pr.pull_request.title            as title,
        pr.pull_request.user.login       as author_login,
        pr.pull_request.state            as state,
        pr.pull_request.merged           as is_merged,
        pr.pull_request.draft            as is_draft,
        to_timestamp(pr.pull_request.created_at) as opened_at,
        to_timestamp(pr.pull_request.closed_at)  as closed_at,
        to_timestamp(pr.pull_request.merged_at)  as merged_at,
        pr.pull_request.additions        as additions,
        pr.pull_request.deletions        as deletions,
        pr.pull_request.changed_files    as changed_files,
        pr.pull_request.commits          as commits_count,
        pr.pull_request.comments         as comments,
        pr.pull_request.review_comments  as review_comments,
        pr.pull_request.author_association as author_association,
        transform(pr.pull_request.labels, l -> l.name) as label_names
    from pr_events

),

ranked as (

    select
        *,
        row_number() over (
            partition by repo_name, pr_number
            order by event_at desc, event_id desc
        ) as rn
    from parsed

)

select
    repo_name,
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    action     as last_action,
    event_at   as last_event_at,
    additions + deletions as churn,
    (unix_timestamp(coalesce(closed_at, event_at)) - unix_timestamp(opened_at)) / 3600.0 as hours_open
from ranked
where rn = 1