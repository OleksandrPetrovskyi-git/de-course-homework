-- Тест: churn у fact_pull_request завжди = additions + deletions.
select *
from {{ ref('fact_pull_request') }}
where churn != additions + deletions