{{ config(materialized='view') }}

select *
from {{ ref('tournaments_datetime') }}