-- Max wind speed, meaned per day
select date_trunc('day', observed) "date", avg(value) wind_max
from observations_aarhus
where parameter = 'wind_max' -- Latest 10 minutes' highest 3 seconds mean wind speed measured 10 m over terrain
and date_trunc('year', observed) = date '2025-01-01'
group by "date"
order by "date";
