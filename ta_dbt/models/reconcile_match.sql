{{ config(materialized='view') }}

with match as (
    select
    match_id
    , tourney_date::DATE as tourney_date
    , winner_name
    , loser_name
    , tourney_name
    , case
        when tourney_name = 'Australian Open' then 'AU'
        when tourney_name = 'Wimbledon' then 'WB'
        when tourney_name = 'Roland Garros' then 'RG'
        when tourney_name = 'Us Open' then 'US'
    end as mapped_tour
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
, join_ as (
    select *
    from match m
    join match_hour mh on m.tourney_date = mh.match_date and mapped_tour = tour_type
)
select * from join_
where winner_name = 'Andy Murray'
and mapped_tour = 'WB'

--24
-- select count(*) from join_
-- where winner_name = 'Andy Murray'
-- 4
-- select count(*) from match
-- where winner_name = 'Andy Murray'
-- select * from join_
-- where player_2 like '%Andy%'
-- (match) Novak Djokovic
-- (match_date) Djokovic N.