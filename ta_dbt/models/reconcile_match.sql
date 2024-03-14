{{ config(materialized='view') }}

with match as (
    select
    match_id
    , tourney_date::DATE AS match_date_atp
    , winner_name
    , loser_name
    , list_sort([winner_name, loser_name]) as names
    , tourney_name
    , tour_abv as tour_atp
    from {{ ref('slv_match_atp') }}
)
, player_mapping as (
    select * from {{ ref('reconcile_player') }}
)
, match_hour as (
    select
    match_date as match_date_hour
    , match_start_hour
    , player_1
    , pm.player_name as player_1_name
    , player_2
    , pm2.player_name as player_2_name
    , list_sort([pm.player_name, pm2.player_name]) as names
    , tour_type as tour_hour
    from {{ ref('slv_match_hour')}} mh
    left join player_mapping pm
    on mh.player_1 = pm.player_name_short
    left join player_mapping pm2
    on mh.player_2 = pm2.player_name_short
    where pm.player_name is not null
    and pm2.player_name is not null

)
, match_consolidate as (--508
select
    concat(names[1], '-', names[2], '-', tour_atp) as composite_id
    , match_id
    , tour_atp
    , match_date_atp
from match
)
, mh_consolidate as (--438
    select
    concat(names[1], '-', names[2], '-', tour_hour) as composite_id
    , match_date_hour
    , match_start_hour
    , tour_hour
    , player_1
    , player_2
    from match_hour
)

select
    m.composite_id
    , m.match_id
    , m.tour_atp
    , mh.tour_hour
    , m.match_date_atp
    , mh.match_date_hour
    , mh.match_start_hour
from
match_consolidate m
inner join
mh_consolidate mh
on mh.composite_id = m.composite_id
