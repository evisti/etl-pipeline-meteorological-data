-- Mean wind speed per day
select date_trunc('day', observed) "date", avg(value) wind_speed
from observations_aarhus
where parameter = 'wind_speed' -- Latest 10 minutes' mean wind speed measured 10 m over terrain
and date_trunc('year', observed) = date '2025-01-01'
group by "date"
order by "date";
