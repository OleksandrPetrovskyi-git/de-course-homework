-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».

with push_events as (

    select
        event_id,
        repo_name,
        actor_login as pushed_by,
        created_at  as pushed_at,
        from_json(payload, '{{ var("push_schema") }}') as push
    from {{ ref('events') }}
    where event_type = 'PushEvent'

),

exploded as (

    select
        event_id,
        repo_name,
        pushed_by,
        pushed_at,
        push.ref as ref,
        commit_item
    from push_events
    lateral view explode(push.commits) as commit_item

),

flattened as (

    select
        commit_item.sha                       as commit_sha,
        repo_name,
        pushed_by,
        regexp_replace(ref, '^refs/heads/', '') as branch,
        commit_item.author.name               as author_name,
        commit_item.author.email              as author_email,
        commit_item.message                   as message,
        commit_item.`distinct`                as is_distinct,
        pushed_at,
        commit_item.message like 'Merge %'    as is_merge_commit,
        split(commit_item.message, '\n')[0]   as message_subject,
        length(commit_item.message)           as message_length,
        event_id
    from exploded

),

deduped as (

    select
        *,
        row_number() over (
            partition by commit_sha
            order by pushed_at, event_id
        ) as rn
    from flattened

)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from deduped
where rn = 1