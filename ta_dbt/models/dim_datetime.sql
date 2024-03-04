{{ config(materialized='table') }}

with srs as (
    select
    strftime(dt, '%Y%m%d%H') as date_id
    , dt::date AS full_date
    , date_part('year', dt) as year
    , date_part('month', dt) as month
    , date_part('day', dt) as day
    , date_part('hour', dt) as hour
    , date_part('dayofweek', dt) as dayofweek
    , date_part('dayofyear', dt) as dayofyear
    from GENERATE_SERIES('2023-01-01'::TIMESTAMP, '2023-12-31'::TIMESTAMP, INTERVAL '1 hour') as t(dt)
)
select * from srs