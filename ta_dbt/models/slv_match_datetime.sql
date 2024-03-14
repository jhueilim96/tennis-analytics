{{ config(materialized='view' )}}

with input as (
    select *
    from {{ ref("tournaments_datetime") }}
)
select *
, match_start::DATE as match_date
, date_part('hour', match_start) as match_start_hour
from input