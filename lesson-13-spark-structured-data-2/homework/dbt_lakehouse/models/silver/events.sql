{{ config(materialized='incremental', incremental_strategy='append') }}

-- Крок 1: silver.events. Специфікація: ../../SPEC.md → «Крок 1».

with base as (

    select
        id                        as event_id,
        type                      as event_type,
        actor.login               as actor_login,
        repo.name                 as repo_name,
        split(repo.name, '/')[0]  as repo_owner,
        to_timestamp(created_at)  as created_at,
        payload,
        public,
        _ingested_at,
        _source_file
    from {{ source('bronze', 'raw_events') }}

    {% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}

),

filtered as (

    select *
    from base
    where event_type in (
            'PushEvent', 'PullRequestEvent', 'IssuesEvent',
            'IssueCommentEvent', 'WatchEvent', 'ForkEvent'
          )
      and public = true
      and event_id is not null
      and repo_name is not null
      and created_at is not null

),

deduped as (

    select
        *,
        row_number() over (
            partition by event_id
            order by _ingested_at, _source_file
        ) as rn
    from filtered

)

select
    event_id,
    event_type,
    actor_login,
    repo_name,
    repo_owner,
    created_at,
    payload,
    _ingested_at,
    _source_file
from deduped
where rn = 1