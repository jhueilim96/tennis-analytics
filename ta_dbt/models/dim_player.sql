{{ config(materialized='table') }}

select *
from {{ ref("reconcile_player") }}