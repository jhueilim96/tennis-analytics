with source_ AS(
    SELECT *
    FROM {{source('atp', 'raw_atp_matches')}}
)
SELECT *
FROM source_