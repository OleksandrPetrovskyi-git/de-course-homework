{{ config(tags=['reconcile']) }}
-- Вітрина не губить і не дублює завершені поїздки: сума rides_completed дорівнює кількості
-- completed у silver.rides. Існуючий assert_agg_matches_fact звіряє вітрину з fact_ride, а цей
-- тест виходить за факт до Silver: ловить і застарілий факт, і застарілий бакет вітрини
-- (поїздка змінила зону, старий рядок не видалено, поїздка порахована двічі).
select a.agg_completed, s.silver_completed
from (
    select coalesce(sum(rides_completed), 0) as agg_completed from {{ ref('agg_zone_hourly') }}
) as a
cross join (
    select count(*) as silver_completed from {{ ref('rides') }} where status = 'completed'
) as s
where a.agg_completed <> s.silver_completed
