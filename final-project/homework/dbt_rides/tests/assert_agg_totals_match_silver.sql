{{ config(tags=['reconcile']) }}
-- Вітрина не губить і не дублює поїздки: суми rides_requested / rides_completed /
-- rides_cancelled дорівнюють кількості поїздок відповідного статусу в silver.rides.
-- Існуючий assert_agg_matches_fact звіряє вітрину з fact_ride, а цей тест виходить за факт
-- до Silver. Головне — rides_requested: застарілий бакет (поїздка змінила зону, старий
-- рядок вітрини не видалено) рахує ту саму поїздку двічі, і сума перевищує кількість поїздок.
select a.*, s.*
from (
    select
        coalesce(sum(rides_requested), 0) as agg_requested,
        coalesce(sum(rides_completed), 0) as agg_completed,
        coalesce(sum(rides_cancelled), 0) as agg_cancelled
    from {{ ref('agg_zone_hourly') }}
) as a
cross join (
    select
        count(*)                                      as silver_requested,
        count(*) filter (where status = 'completed')  as silver_completed,
        count(*) filter (where status = 'cancelled')  as silver_cancelled
    from {{ ref('rides') }}
) as s
where a.agg_requested <> s.silver_requested
   or a.agg_completed <> s.silver_completed
   or a.agg_cancelled <> s.silver_cancelled
