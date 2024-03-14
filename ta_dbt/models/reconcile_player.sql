with mdt as (
    select * from {{ ref("slv_match_datetime") }}
)
, mdt_player as (
    select player_1 as player from mdt
    union
    select player_2 as player from mdt
)
, dedupe as (
    select
    player as name_short
    , replace(player, '.', ' ') as name_clean_short
    from mdt_player group by (player) order by player
)
, mapping as (
    select * from {{ ref('player_mapping') }}
)
, dim_player as (
    select * from {{ ref("dim_player") }}
)
select
d.player_id as player_id
, d.player_name as player_name
, i.name_short as player_name_short
from dedupe i
inner join mapping m on i.name_clean_short = m.player_name_input
inner join dim_player d on  m.player_name_match = d.player_name