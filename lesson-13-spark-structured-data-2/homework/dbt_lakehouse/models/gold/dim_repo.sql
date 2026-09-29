-- Крок 5: gold.dim_repo. Специфікація: ../../SPEC.md → «Крок 5».

select
    md5(repo_name)   as repo_id,
    repo_name,
    max(repo_owner)  as repo_owner,
    min(created_at)  as first_seen_at,
    max(created_at)  as last_seen_at,
    count(*)         as event_count,
    sum(case when event_type = 'ForkEvent' then 1 else 0 end) > 0 as is_forked
from {{ ref('events') }}
group by repo_name