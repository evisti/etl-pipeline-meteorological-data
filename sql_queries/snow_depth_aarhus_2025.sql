-- Snow depth in cm per day
select observed "date", value snow
from observations_aarhus
where parameter = 'snow_depth_man'
and date_trunc('year', observed) = date '2025-01-01'
order by "date";
