{{ config(tags=['reconcile']) }}
-- driver_key у fact_ride збігається з водієм поїздки: 'unknown' лише тоді, коли в silver.rides
-- водія нема (скасовано до прийняття), і навпаки. Ловить інкремент, що склав ключ у 'unknown'
-- раніше, ніж водій зʼявився в dim_driver, і не оновив факт. Модель із двох шарів, тому тег
-- reconcile: під `--indirect-selection cautious` без нього тест не запустився б ніде.
select r.ride_id, r.driver_id, f.driver_key
from {{ ref('rides') }} as r
inner join {{ ref('fact_ride') }} as f on f.ride_id = r.ride_id
where (r.driver_id is null) <> (f.driver_key = 'unknown')
   or (r.driver_id is not null and f.driver_key <> r.driver_id)
