-- Accumulated precipitation in mm per day
select date_trunc('day', observed) "date", sum(value) accum_precip
from observations_aarhus
where parameter = 'precip_past10min' 
and date_trunc('year', observed) = date '2025-01-01'
group by "date"
order by "date";
