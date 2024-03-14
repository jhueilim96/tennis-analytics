{{ config(materialized='view') }}

select *
from {{ ref("reconcile_player") }}