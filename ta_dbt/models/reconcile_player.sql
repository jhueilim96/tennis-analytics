{{ config(materialized='view') }}

with ply_hour as (
    select
    name_short
    , name_clean_short
    from {{ ref("slv_player_match_hour") }}
)
, mapping as (
    select * from {{ ref('player_mapping') }}
)
, ply_atp as (
    select * from {{ ref("slv_player_match_atp") }}
)
select
d.player_id as player_id
, d.player_name as player_name
, i.name_short as player_name_short
, name_short
, name_clean_short
from ply_hour i
inner join mapping m on i.name_clean_short = m.player_name_input
inner join ply_atp d on  m.player_name_match = d.player_name