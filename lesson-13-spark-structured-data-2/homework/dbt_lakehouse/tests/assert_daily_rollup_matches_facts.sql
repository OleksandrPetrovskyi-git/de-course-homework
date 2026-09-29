-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
select *
from (
    select sum(commits) as rollup_commits from {{ ref('fact_repo_activity_daily') }}
) rollup
cross join (
    select count(*) as fact_commits from {{ ref('fact_commit') }}
) fact
where rollup.rollup_commits != fact.fact_commits