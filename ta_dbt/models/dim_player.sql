with win as (
    select
    winner_id as player_id
    , winner_name as player_name
    , winner_ioc as country
    , winner_age as age
    , winner_ht as height
    , winner_rank as rank
    , winner_rank_points as points
    from {{ ref("tournaments") }}
), lose as (
    select
    loser_id as player_id
    , loser_name as player_name
    , loser_ioc as country
    , loser_age as age
    , loser_ht as height
    , loser_rank as rank
    , loser_rank_points as points
    from {{ ref("tournaments") }}
), union_ as (
    select  * from win
    union all
    select * from lose
) , dedupe as (
    select
    player_id
    , any_value(player_name) as player_name
    , any_value(country) as country
    , any_value(age) as age
    , any_value(height) as height
    , any_value(rank) as rank
    , any_value(points) as points
    from union_
    group by (player_id)
)
select
player_id
, player_name
, split_part(player_name, ' ', 1) as first_name
, split_part(player_name, ' ', -1) as last_name
, country
, age
, height
, rank
, points
from dedupe