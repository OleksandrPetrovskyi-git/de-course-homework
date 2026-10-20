{{ config(materialized='table') }}

-- gold.dim_zone. Grain: зона. SPEC.md, розділ 4.4.
-- Статичний довідник із seed, тому table, а не incremental. Порожні значення -> 'Unknown';
-- додатковий член -1 для поїздок, чия зона відсутня у довіднику.

select
    location_id::int                                   as zone_key,
    coalesce(nullif(trim(borough), ''), 'Unknown')     as borough,
    coalesce(nullif(trim(zone_name), ''), 'Unknown')   as zone_name,
    coalesce(nullif(trim(service_zone), ''), 'Unknown') as service_zone
from {{ ref('seed_taxi_zone') }}

union all

select
    -1             as zone_key,
    'Unknown'      as borough,
    'Unknown'      as zone_name,
    'Unknown'      as service_zone
