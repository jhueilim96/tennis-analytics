{{ config(materialized='view' )}}

with wea as (
    select
    datetime::date as date
    , date_part('hour', datetime) as hour
    , temperature
    , humidity
    , precipitation
    , cloud_cover
    , wind_speed
    , wind_direction
    , soil_temperature
 from {{ ref('weather') }}
)
, match_ as (
    select * from {{ ref('reconcile_match') }}
)
select
match_id
, date
, hour
, temperature
, humidity
, precipitation
, cloud_cover
, wind_speed
, wind_direction
, soil_temperature
, tourney_name
from
    wea w
inner join
    match_ m
on
    w.date=m.match_date
and
    w.hour=m.match_start_hour