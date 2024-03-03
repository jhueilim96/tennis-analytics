with weather_union as (
    select *, 'New York' as loc
    from {{ source('atp', 'raw_weather_ny')}}
    union all
    select *, 'Paris' as loc from {{ source('atp', 'raw_weather_paris')}}
    union all
    select *, 'London' as loc from {{ source('atp', 'raw_weather_london')}}
    union all
    select *, 'Merlbourne' as loc from {{ source('atp', 'raw_weather_melbourne')}}
),
rename as (
select
    strptime("time", '%Y-%m-%dT%H:%M') as datetime
    ,"temperature_2m (°C)" as temperature
    ,"relative_humidity_2m (%)" as humidity
    ,"precipitation (mm)" as precipitation
    ,"cloud_cover (%)" as cloud_cover
    ,"wind_speed_10m (km/h)" as wind_speed
    ,"wind_direction_10m (°)" as wind_direction
    ,"soil_temperature_0_to_7cm (°C)" as soil_temperature
    ,"loc"
from weather_union
)
select * from rename