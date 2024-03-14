with raw_ as (
    select     tourney_id
    , match_num
    , tourney_name
    , tour_abv
    , tourney_date
    , tourney_level
    , winner_name
    , loser_name
    , score
    , best_of
    , round
    , "minutes"
    from {{ ref("slv_match_atp") }}
)
select
*
from raw_