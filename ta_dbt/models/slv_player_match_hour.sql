{{ config(materialized='view') }}

with mdt as (
    select * from {{ ref("tournaments_datetime") }}
)
, mdt_player as (
    select player_1 as player, tour_type from mdt
    union
    select player_2 as player, tour_type from mdt
)
, dedupe as (
    select
    player as name_short
    , replace(player, '.', ' ') as name_clean_short
    from mdt_player group by (player) order by player
)
select *
from dedupe
