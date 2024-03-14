{{ config(materialized='table') }}

with win as (
    select
    match_id
    , winner_id as player_id
    , tourney_date
    , True as is_win
    , w_ace as ace
    , w_df as df
    , w_svpt as svpt
    , w_1stIn as "1stIn"
    , w_1stWon as "1stWon"
    , w_2ndWon as "2ndWon"
    , w_SvGms as SvGms
    , w_bpSaved as bpSaved
    , w_bpFaced as bpFaced
    from {{ ref("slv_match_atp") }}
), lose as (
    select
    match_id
    , loser_id as player_id
    , tourney_date
    , False as is_win
    , l_ace as ace
    , l_df as df
    , l_svpt as svpt
    , l_1stIn as "1stIn"
    , l_1stWon as "1stWon"
    , l_2ndWon as "2ndWon"
    , l_SvGms as SvGms
    , l_bpSaved as bpSaved
    , l_bpFaced as bpFaced
    from {{ ref("slv_match_atp") }}
), union_ as (
    select * from win
    union all
    select * from lose
)
, reconcile_match as (
    select
    match_id
    , match_date_hour as match_date
    , match_start_hour as match_start_hour
    from {{ ref('reconcile_match') }}
)
, weather as (
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
, kpi_weather as (
select
    u.match_id
    , u.player_id
    , rm.match_date
    , rm.match_start_hour
    , is_win
    , ace
    , df
    , svpt
    , "1stIn"
    , "1stWon"
    , "2ndWon"
    , SvGms
    , bpSaved
    , bpFaced
    , temperature
    , humidity
    , precipitation
    , cloud_cover
    , wind_speed
    , wind_direction
    , soil_temperature
from union_ u
join
    reconcile_match rm
    on
        u.match_id=rm.match_id
join
    weather w
    on
        w.date=rm.match_date
    and
        w.hour=rm.match_start_hour
)
select *
from kpi_weather