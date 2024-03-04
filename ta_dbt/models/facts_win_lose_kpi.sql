{{ config(materialized='table') }}

with win as (
    select
    match_id
    , winner_id as player_id
    , tourney_date
    , True as is_win
    , w_ace as ace
    , w_df as df
    , w_svpt as svpt
    , w_1stIn as "1stIn"
    , w_1stWon as "1stWon"
    , w_2ndWon as "2ndWon"
    , w_SvGms as SvGms
    , w_bpSaved as bpSaved
    , w_bpFaced as bpFaced
    from {{ ref("slv_match") }}
), lose as (
    select
    match_id
    , loser_id as player_id
    , tourney_date
    , False as is_win
    , l_ace as ace
    , l_df as df
    , l_svpt as svpt
    , l_1stIn as "1stIn"
    , l_1stWon as "1stWon"
    , l_2ndWon as "2ndWon"
    , l_SvGms as SvGms
    , l_bpSaved as bpSaved
    , l_bpFaced as bpFaced
    from {{ ref("slv_match") }}
), union_ as (
    select * from win
    union all
    select * from lose
)
select * from union_
