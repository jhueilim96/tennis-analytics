with au AS(
    SELECT
    "Date", "Time", "Player 1", "Player 2", 'AU' as tour_type
    FROM {{source('atp', 'raw_match_date_auopen')}}
), rg as (
    SELECT "Date", Timestamp as "Time", "Player 1", "Player 2", 'RG' as tour_type
    FROM {{ source('atp', 'raw_match_date_rg')}}
), us1 as (
    SELECT
    "Date", Timestamp as "Time", "Player 1", "Player 2", 'US' as tour_type
    FROM {{ source('atp', 'raw_match_date_usopen')}}
), w1 as (
    SELECT
    "Date", "Time", "Player 1", "Player 2", 'WB' as tour_type
    FROM {{ source('atp', 'raw_match_date_wimby')}}
), w2 as (
    SELECT
    "Date", "Time", "Player 1", "Player 2", 'WB' as tour_type
    FROM {{ source('atp', 'raw_match_date_wimby2')}}
), union_tour as (
    select * from au
    union all
    select * from rg
    union all
    select * from us1
    union all
    select * from w1
    union all
    select * from w2
),
data_type_norm as (
    SELECT
    strptime(("Date"::VARCHAR) || 'T' ||"Time", '%Y%m%dT%H:%M') as datetime
    , "Player 1" as player_1
    , "Player 2" as player_2
    , tour_type
    FROM union_tour
)
select
    (time_bucket(INTERVAL '1 hour', datetime)) - INTERVAL '3 hours' as match_start
    , player_1
    , player_2
    , tour_type
from data_type_norm