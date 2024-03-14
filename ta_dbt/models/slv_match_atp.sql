{{ config(materialized='view') }}

with raw_ as (
    select
    *
    , case
        when tourney_name = 'Australian Open' then 'AU'
        when tourney_name = 'Wimbledon' then 'WB'
        when tourney_name = 'Roland Garros' then 'RG'
        when tourney_name = 'Us Open' then 'US'
    end as tour_abv
    from {{ ref("tournaments") }}
    where tourney_name in ('Australian Open', 'Wimbledon', 'Roland Garros', 'Us Open')
)
select
(tourney_id || '-' || match_num::VARCHAR) as match_id
, *
from raw_