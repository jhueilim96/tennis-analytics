with source_ AS(
    SELECT * 
    FROM {{source('atp', 'raw_atp_matches')}}
), 
select_column AS(
    SELECT 
    tourney_id
    , tourney_name  
    , surface
    FROM source_
)
SELECT * 
FROM select_column