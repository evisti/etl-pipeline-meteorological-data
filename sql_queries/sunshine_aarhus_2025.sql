-- Number of minutes with sunshine per day
select date_trunc('day', observed) "date", sum(value) sunshine
from observations_aarhus
where parameter = 'sun_last10min_glob' 
and date_trunc('year', observed) = date '2025-01-01'
group by "date"
order by "date";
