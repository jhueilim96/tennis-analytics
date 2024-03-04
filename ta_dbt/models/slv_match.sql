{{ config(materialized='view') }}

with raw_ as (
    select
    *
    from {{ ref("tournaments") }}
    where tourney_name in ('Australian Open', 'Wimbledon', 'Roland Garros', 'Us Open')
)
select
(tourney_id || '-' || match_num::VARCHAR) as match_id
, *
from raw_