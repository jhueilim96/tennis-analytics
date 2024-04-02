with source_ AS(
    SELECT * exclude (tourney_date)
    , strptime( (tourney_date:: VARCHAR), '%Y%m%d') as tourney_date
    FROM {{source('atp', 'raw_atp_matches')}}
)
SELECT *
FROM source_