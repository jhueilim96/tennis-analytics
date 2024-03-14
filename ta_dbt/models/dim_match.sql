with raw_ as (
    select     tourney_id
    , match_num
    , tourney_name
    , tourney_date
    , tourney_level
    , winner_name
    , loser_name
    , score
    , best_of
    , round
    , "minutes"
    from {{ ref("slv_match") }}
)
select
*
from raw_