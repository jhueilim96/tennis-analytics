{{ config(materialized='table') }}

with match as (
    select
    match_id
    , tourney_date--::DATE as tourney_date
    , winner_name
    , loser_name
    , list_sort([winner_name, loser_name]) as names
    , tourney_name
    from {{ ref('slv_match') }}
), match_hour as (
    select
    match_date
    , match_start_hour
    , player_1
    , player_2
    , tour_type
    from {{ ref('slv_match_datetime')}}
)
, player_mapping as (
    select * from {{ ref('reconcile_player') }}
)
, match_consolidate as (--508
select
    concat(names[1], '-', names[2], '-', strftime(tourney_date, '%Y/%m/%d')) as composite_id
    , match_id
    , tourney_date
    , tourney_name
from match
)
, mh_pre_conso as (
    select
    match_date
    , match_start_hour
    , player_1
    , pm.player_name as player_1_name
    , player_2
    , pm2.player_name as player_2_name
    , list_sort([pm.player_name, pm2.player_name]) as names
    from match_hour mh
    left join player_mapping pm
    on mh.player_1 = pm.player_name_short
    left join player_mapping pm2
    on mh.player_2 = pm2.player_name_short
    where pm.player_name is not null
    and pm2.player_name is not null
)
, mh_consolidate as (--438
    select
    concat(names[1], '-', names[2], '-', strftime(match_date, '%Y/%m/%d')) as composite_id
    , match_date
    , match_start_hour
    , player_1
    , player_2
    from mh_pre_conso
)
select
    m.composite_id
    , match_id
    , tourney_name
    , match_date
    , match_start_hour
from
match_consolidate m
inner join
mh_consolidate mh
on mh.composite_id = m.composite_id
